package Plack::App::PgREST;
use 5.12.1;
use methods;
use 5.008_001;
our $VERSION = '0.01';
use Plack::Builder;
use Router::Resource;
use parent qw(Plack::Component);
use Plack::Util::Accessor qw( dsn dbh );
use Plack::Request;
use JSON qw(encode_json);

method select($req, $args) {
    use Data::Dumper;
    my $req = encode_json({collection => $args->{collection}, l => scalar $req->param('l'), sk => scalar $req->param('sk'), c => scalar $req->param('c')});
    warn $req;
    my $ary_ref = $self->{dbh}->selectall_arrayref("select postgrest_select(?)", {}, $req);
    return [200, ['Content-Type', 'application/json'], [$ary_ref->[0][0]]];
}

method _mk_func($name, $param, $ret, $body, $lang) {
    my (@params, @args);
    $lang ||= 'plv8';

    while( my ($name, $type) = splice(@$param, 0, 2) ) {
        push @params, "$name $type";
        push @args, $name;
    }

    qq{CREATE OR REPLACE FUNCTION $name (@{[ join(',', @params) ]}) RETURNS $ret AS \$\$
return ($body)(@{[ join(',', @args) ]});
\$\$ LANGUAGE plls IMMUTABLE STRICT;}
}

method bootstrap {
    $self->{dbh}->do($self->_mk_func("postgrest_select", [req => "json"], "json", << 'END'));
({collection, l = 30, sk = 0, q, c}) ->
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
            GET  { $self->select(Plack::Request->new($_[0]), $_[1]) };
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
