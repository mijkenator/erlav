-module(named_type_ref_test).

-include_lib("eunit/include/eunit.hrl").

%% Regression tests for gaps found while auditing #14's fix for named-type
%% references inside array-of-union items (resolve_named_type_refs /
%% collect_named_types in c_src/mkh_avro2.hh). #14 itself only covered a
%% bare-string reference to a record inside a union; these tests cover
%% the other shapes the same resolver is meant to handle: a bare
%% reference to a named *enum* (not just a record), a bare reference in a
%% plain (non-union) "items"/"values" slot, and a reference resolved
%% against a namespace inherited from an *enclosing* nested record rather
%% than the top-level schema's own namespace.

%% A named enum, defined once, referenced by bare name from a second
%% plain field and again from inside an array-items union -- both must
%% resolve via collect_named_types/lookup_named_type the same way a named
%% record does. See test/tschema_named_enum_ref.avsc.
named_enum_reference_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_named_enum_ref.avsc">>),
    Term = #{
        <<"colorField">> => <<"RED">>,
        <<"secondColorField">> => <<"BLUE">>,
        <<"arrayField">> => [1, <<"GREEN">>, 2]
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1),
    ok.

%% A named record, defined once, referenced by a bare "items"/"values"
%% string outside of any union -- {"type":"array","items":"Point"} and
%% {"type":"map","values":"Point"} -- distinct from both the
%% self-referential case (tschema_self_ref.avsc) and the
%% array-items-union case (tschema_array_of_union7.avsc) #14 already
%% covers. See test/tschema_named_items_ref.avsc.
named_items_values_reference_test() ->
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_named_items_ref.avsc">>),
    Term = #{
        <<"definingField">> => #{<<"x">> => 1, <<"y">> => 2},
        <<"pointsField">> => [
            #{<<"x">> => 10, <<"y">> => 20},
            #{<<"x">> => 30, <<"y">> => 40}
        ],
        <<"pointsMapField">> => #{
            <<"a">> => #{<<"x">> => 1, <<"y">> => 1},
            <<"b">> => #{<<"x">> => 2, <<"y">> => 2}
        }
    },
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Encoded: ~p ~n", [Encoded]),
    ?assert(is_binary(Encoded)),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, Encoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assert(true == tst_utils:compare_maps_deep(Term, Re1)),
    ok.

%% A record nested inside another record declares its own "namespace",
%% overriding the top-level schema's namespace for itself and everything
%% nested inside it. A sibling top-level field then references the
%% doubly-nested record by its fullname under that *child* namespace
%% (erlav.test.nested.Inner), not the top-level schema's namespace
%% (erlav.test). resolve_named_type_refs's child_namespace inheritance
%% must track this correctly. Cross-validated against erlavro's own
%% encoder for byte-level correctness (unlike the enum test above, this
%% shape doesn't hit the negative-block-count gap tracked in #15, since
%% there's no array/map involved). See
%% test/tschema_nested_namespace_ref.avsc.
nested_namespace_reference_test() ->
    {ok, SchemaJSON} = file:read_file("test/tschema_nested_namespace_ref.avsc"),
    Encoder = avro:make_simple_encoder(SchemaJSON, []),
    SchemaId = erlav_nif:erlav_init(<<"test/tschema_nested_namespace_ref.avsc">>),
    Term = #{
        <<"wrapperField">> => #{<<"inner">> => #{<<"value">> => 42}},
        <<"secondInnerField">> => #{<<"value">> => 99}
    },
    ErlavroEncoded = iolist_to_binary(Encoder(Term)),
    ErlavEncoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?debugFmt("Erlavro: ~p ~n", [ErlavroEncoded]),
    ?debugFmt("Erlav:   ~p ~n", [ErlavEncoded]),
    ?assertEqual(ErlavroEncoded, ErlavEncoded),
    Re1 = erlav_nif:erlav_decode_fast(SchemaId, ErlavEncoded),
    ?debugFmt("decode result: ~p ~n", [Re1]),
    ?assertEqual(Term, Re1),
    ok.
