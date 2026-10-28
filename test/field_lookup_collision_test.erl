-module(field_lookup_collision_test).

%% encoderecord's single-pass path (records with more than a few fields)
%% resolves each input-map key to a field index by name through
%% SchemaItem::find_field_index. These tests pin down that lookup against
%% names that are easy to confuse, so any replacement of the lookup (e.g. a
%% cheaper hash or a custom table that only inspects part of the key) cannot
%% silently map a key to the wrong field. test/colliding_field_names.avsc
%% contains:
%%   * four 26-byte names that differ only in the middle (same length, same
%%     first/last 8 bytes), plus a 27-byte one that shares their prefix and
%%     suffix,
%%   * names under 8 bytes that are prefixes of each other,
%%   * 9-15 byte names differing by a single byte or by length only.
%% Every field must land on its own slot, and an input key that resembles a
%% field but is not one must be ignored.

-include_lib("eunit/include/eunit.hrl").

-define(SCHEMA, <<"test/colliding_field_names.avsc">>).

fields() ->
    [<<"prefix00_AAAAAAAA_suffix00">>,
     <<"prefix00_BBBBBBBB_suffix00">>,
     <<"prefix00_CCCCCCCC_suffix00">>,
     <<"prefix00_DDDDDDDD_suffix00">>,
     <<"prefix00_AAAAAAAAA_suffix00">>,
     <<"a">>, <<"ab">>, <<"abc">>, <<"abcdefg">>, <<"abcdefgh">>,
     <<"abcdefghi">>, <<"abcdefgXhi">>,
     <<"abcdefghijklmno">>, <<"abcdefgXijklmno">>].

%% Each field gets a distinct value, so a field resolved to the wrong slot
%% shows up as the wrong byte (or a swapped decode).
term(Values) ->
    maps:from_list(lists:zip(fields(), Values)).

expected_bytes(Values) ->
    %% All values are small non-negative ints: zigzag is V*2, one byte each,
    %% emitted in schema field order.
    << <<(V * 2)>> || V <- Values >>.

all_fields_resolve_test() ->
    SchemaId = erlav_nif:erlav_init(?SCHEMA),
    Values = lists:seq(1, length(fields())),
    Encoded = erlav_nif:erlav_encode(SchemaId, term(Values)),
    ?assertEqual(expected_bytes(Values), Encoded),
    ?assertEqual(term(Values), erlav_nif:erlav_decode_fast(SchemaId, Encoded)).

%% Same fields, values assigned in reverse: guards against a lookup that
%% happens to be right only because insertion order matches schema order.
reversed_values_test() ->
    SchemaId = erlav_nif:erlav_init(?SCHEMA),
    Values = lists:reverse(lists:seq(1, length(fields()))),
    Encoded = erlav_nif:erlav_encode(SchemaId, term(Values)),
    ?assertEqual(expected_bytes(Values), Encoded).

%% Keys that are not schema fields but collide with one (same length and
%% same first/last 8 bytes, or a prefix/extension of a field name) must be
%% ignored, not mistaken for the field they collide with.
colliding_unknown_keys_ignored_test() ->
    SchemaId = erlav_nif:erlav_init(?SCHEMA),
    Values = lists:seq(1, length(fields())),
    Extras = #{<<"prefix00_ZZZZZZZZ_suffix00">> => 99,
               <<"prefix00_AAAAAAAAAA_suffix00">> => 98,
               <<"prefix00_AAAAAAAA_suffix01">> => 97,
               <<"abcdefgXXXhi">> => 96,
               <<"abcdefgZijklmno">> => 95,
               <<"abcdefghij">> => 94,
               <<"abcd">> => 93,
               <<>> => 92},
    Term = maps:merge(term(Values), Extras),
    Encoded = erlav_nif:erlav_encode(SchemaId, Term),
    ?assertEqual(expected_bytes(Values), Encoded).

%% Many records sharing the colliding layout, with varying values, so a
%% lookup bug that only bites for some key orderings in the input map is
%% likely to be hit.
randomized_values_test() ->
    SchemaId = erlav_nif:erlav_init(?SCHEMA),
    N = length(fields()),
    lists:foreach(
      fun(_) ->
              Values = [rand:uniform(60) || _ <- lists:seq(1, N)],
              Encoded = erlav_nif:erlav_encode(SchemaId, term(Values)),
              ?assertEqual(expected_bytes(Values), Encoded)
      end, lists:seq(1, 200)).
