package Plack::App::PgREST;
use methods;
use 5.008_001;
our $VERSION = '0.01';
use Plack::Builder;
use Router::Resource;
use parent qw(Plack::Component);
use Plack::Util::Accessor qw( dsn dbh pg_version );
use Plack::Request;
use JSON qw(encode_json);
use IPC::Run3 qw(run3);

method select($param, $args) {
    use Data::Dumper;
    my $req = encode_json({
        collection => $args->{collection},
        l => $param->get('l'),
        sk => $param->get('sk'),
        c => $param->get('c'),
    });
    my $ary_ref = $self->{dbh}->selectall_arrayref("select postgrest_select(?)", {}, $req);
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
create extension if not exists plv8;
create extension if not exists plls;
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
({collection, l = 30, sk = 0, q, c, q, fo}) ->
    query = "select * from #collection"
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
