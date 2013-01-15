package Plack::App::PgREST;
use methods;
use 5.008_001;
our $VERSION = '0.01';
use Plack::Builder;
use Router::Resource;
use parent qw(Plack::Component);
use Plack::Util::Accessor qw( dsn dbh pg_version );
use Plack::Request;
use JSON qw(encode_json decode_json);
use IPC::Run3 qw(run3);

sub n {
    my $n = shift;
    return $n unless $n;
    return undef if $n eq 'NaN';
    return int($n);
}

method select($param, $args) {
    use Data::Dumper;
    my $req = encode_json({
        collection => $args->{collection},
        l => n($param->get('l')),
        sk => n($param->get('sk')),
        c => n($param->get('c')),
        q => decode_json($param->get('q') || undef),
    });
    my $ary_ref = $self->{dbh}->selectall_arrayref("select postgrest_select(?)", {}, $req);
    if (my $callback = $param->get('callback')) {
        $callback =~ s/[^\w\[\]\.]//g;
        return [200, ['Content-Type', 'application/javascript'],
            [
                ";($callback)($ary_ref->[0][0]);"
            ]
        ];
    }
    return [200, ['Content-Type', 'application/json'], [$ary_ref->[0][0]]];
}

method _mk_func($name, $param, $ret, $body, $lang, $dont_compile) {
    my (@params, @args);
    $lang ||= 'plv8';
    while( my ($name, $type) = splice(@$param, 0, 2) ) {
        push @params, "$name $type";
        if ($type eq 'pgrest_json') {
            push @args, "JSON.parse($name)"
        }
        else {
            push @args, $name;
        }
    }

    my $compiled = '';
    if ($lang eq 'plls' && !$dont_compile) {
        $lang = 'plv8';

        $compiled = $self->{dbh}->selectall_arrayref("select jsapply(?,?)", {}, "LiveScript.compile", encode_json([$body, {bare =>  true}]))->[0][0];
        $compiled =~ s/;$//;
    }

    $compiled ||= $body;
    $body = "JSON.stringify((eval($compiled))(@{[ join(',', @args) ]}));";
#    $body = ($self->{pg_version} lt '9.2.0')
#        ? "JSON.stringify(($compiled)(@{[ join(',', map { qq!JSON.parse($_)! } @args) ]}));"
#        : "($compiled)(@{[ join(',', @args) ]})";
    my $ret = qq{CREATE OR REPLACE FUNCTION $name (@{[ join(',', @params) ]}) RETURNS $ret AS \$\$
return $body
\$\$ LANGUAGE $lang IMMUTABLE STRICT;};
    warn $ret;
    $ret;
}

method bootstrap {

    ($self->{pg_version}) = $self->{dbh}->selectall_arrayref("select version()")->[0][0] =~ m/PostgreSQL ([\d\.]+)/;
    if ($self->{pg_version} ge '9.1.0') {
        $self->{dbh}->do(<<'EOF');
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS plv8;
EXCEPTION WHEN OTHERS THEN END; $$;
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS plls;
EXCEPTION WHEN OTHERS THEN END; $$;
EOF
    }
    else {
        # bootstrap with sql
        
    }
    
    
    if ($self->{pg_version} lt '9.2.0') {
        $self->{dbh}->do(<<'EOF');
DO $$ BEGIN
    CREATE FUNCTION json_syntax_check(src text) RETURNS boolean AS '
        try { JSON.parse(src); return true; } catch (e) { return false; }
    ' LANGUAGE plv8 IMMUTABLE;
EXCEPTION WHEN OTHERS THEN END; $$;

DO $$ BEGIN
    CREATE DOMAIN pgrest_json AS text CHECK ( json_syntax_check(VALUE) );
EXCEPTION WHEN OTHERS THEN END; $$;
EOF
    }
    else {
        $self->{dbh}->do(<<'EOF');
DO $$ BEGIN
    CREATE DOMAIN pgrest_json AS json;
EXCEPTION WHEN OTHERS THEN END; $$;
EOF
    }

    $self->{dbh}->do($self->_mk_func("jseval", [str => "text"], "text", << 'END', 'plv8'));
function(str) { return eval(str) }
END

    $self->{dbh}->do($self->_mk_func("jsapply", [str => "text", "args" => "pgrest_json"], "pgrest_json", << 'END', 'plv8'));
function (func, args) {
    return eval(func).apply(null, args);
}
END

    my $ls = do { local $/; open my $fh, '<', 'livescript.js'; <$fh> };
    $ls =~ s/\$\$/\\\$\\\$/g;
    $self->{dbh}->do($self->_mk_func("lsbootstrap", [], "pgrest_json", << "END", 'plv8'));
function() { jsid = 0; LiveScript = $ls }
END
    $self->{dbh}->do("select lsbootstrap()");

    $self->{dbh}->do($self->_mk_func("jsevalit", [str => "text"], "text", << 'END', 'plls'));
(str) ->
    ``jsid = jsid || 0; ++jsid;``
    eval "jsid#jsid = " + str; "jsid#jsid"
END

    $self->{dbh}->do($self->_mk_func("postgrest_select", [req => "pgrest_json"], "pgrest_json", << 'END', 'plls'));

q = -> """
    '#{ "#it".replace /'/g "''" }'
"""
qq = -> """
    "#{ "#it".replace /"/g '""' }"
"""

walk = (model, meta) ->
    return [] unless meta?[model]
    for col, spec of meta[model]
        [compile(model, spec), col]

compile = (model, field) ->
    {$query, $from, $and, $} = field ? {}
    switch
    | $from? => """
        (SELECT COALESCE(ARRAY_TO_JSON(ARRAY_AGG(_)), '[]') FROM (SELECT * FROM #from-table
            WHERE #{ qq "_#model" } = #model-table."_id" AND #{
                switch
                | $query?                   => cond model, $query
                | _                         => true
            }
        ) AS _)
    """ where from-table = qq "#{$from}s", model-table = qq "#{model}s"
    | $? => cond model, $
    | typeof field is \object => cond model, field
    | _ => field

cond = (model, spec) -> switch typeof spec
    | \number => spec
    | \string => qq spec
    | \object =>
        # Implicit AND on all k,v
        [ test model, qq(k), v for k, v of spec ] * "AND"
    | _ => it

test = (model, key, expr) -> switch typeof expr
    | <[ number boolean ]> => "(#key = #expr)"
    | \string => "(#key = #{ q expr })"
    | \object => for op, ref of expr
        switch op
            | \$gt =>
                res = evaluate model, ref
                return "(#key > #res)"
            | \$ =>
                return "#key = #model-table.#{ qq ref }" where model-table = qq "#{model}s"
            | _ => throw "Unknown operator: #op"
    | \undefined => true

evaluate = (model, ref) -> switch typeof ref
    | <[ number boolean ]> => "#ref"
    | \string => q #ref
    | \object => for op, v of ref => switch op
        | \$ => "#model-table.#{ qq v }" where model-table = qq "#{model}s"
        | \$ago => "'now'::timestamptz - #{ q "#v ms" }::interval"
        | _ => continue

#module.exports = exports = { walk, compile }



({collection, l = 30, sk = 0, q, c, q, fo}) ->
    var cond
    if q
        cond = compile collection, q
    query = "select * from #collection"
    query += " WHERE #cond" if cond?
    [{count}] = plv8.execute "select count(*) from (#query) cnt"
    return { count } if c

    do
        paging: { count, l, sk }
        entries: plv8.execute "#query limit $1 offset $2" [l, sk]
END
}

method to_app {
    warn $self->can('to_app');
    warn $self->{dsn};
    unless ($self->{dbh}) {
        require DBIx::Connector;
        $self->{conn} = DBIx::Connector->new($self->{dsn}, '', '', {
          RaiseError => 1,
          AutoCommit => 1,
        });
        $self->{dbh} = $self->{conn}->dbh;
    }
    die unless $self->{dbh};
    $self->bootstrap;
    my $router = router {
        resource '/collections/{collection}' => sub {
            GET  { $self->select(Plack::Request->new($_[0])->parameters, $_[1]) };
        };
    };

    builder {
        sub { $router->dispatch(shift) };
    };
}

1;
__END__

=encoding utf-8

=for stopwords

=head1 NAME

Plack::App::PgREST -

=head1 SYNOPSIS

  use Plack::App::PgREST;

=head1 DESCRIPTION

Plack::App::PgREST is

=head1 AUTHOR

Chia-liang Kao E<lt>clkao@clkao.orgE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

=cut
