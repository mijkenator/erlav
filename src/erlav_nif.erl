-module(erlav_nif).

-export([
    erlav_init/1,
    erlav_encode/2,
    erlav_decode/2,
    erlav_safe_encode/2,
    replace_keys/1
]).

-nifs([erlav_init/1, erlav_encode/2, erlav_decode/2]).

-on_load(init/0).

-define(APPNAME, erlav_nif).
-define(LIBNAME, erlav_nif).

init() ->
    SoName =
        case code:priv_dir(?APPNAME) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, ?LIBNAME]);
                    _ ->
                        filename:join([priv, ?LIBNAME])
                end;
            Dir ->
                filename:join(Dir, ?LIBNAME)
        end,
    erlang:load_nif(SoName, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

erlav_encode(_A, _B) ->
    not_loaded(?LINE).

erlav_decode(_A, _B) ->
    not_loaded(?LINE).

erlav_init(_A) ->
    not_loaded(?LINE).

erlav_safe_encode(SchemaId, Data) ->
    erlav_encode(SchemaId, replace_keys(Data)).

replace_keys(Term) when is_map(Term) ->
    maps:fold(
        fun
            (_, [], AccIn) -> AccIn;
            (_, null, AccIn) -> AccIn;
            (K, V, AccIn) -> maps:put(key_to_binary(K), replace_keys(V), AccIn)
        end,
        #{},
        Term
    );
replace_keys([{_, _} | _] = Lst) ->
    replace_keys(maps:from_list(Lst));
replace_keys(L) when is_list(L) -> [replace_keys(Ele) || Ele <- L];
replace_keys(Term) ->
    value_to_binary(Term).

value_to_binary([L1 | _] = L) when is_integer(L1) ->
    case is_string(L) of
        true -> list_to_binary(L);
        _ -> L
    end;
value_to_binary(V) ->
    V.

key_to_binary(A) when is_atom(A) -> atom_to_binary(A);
key_to_binary([L1 | _] = L) when is_integer(L1) -> list_to_binary(L);
key_to_binary(K) -> K.

isprint(X) when X >= 32, X < 127 -> true;
isprint(_) -> false.
is_string(List) when is_list(List) -> lists:all(fun isprint/1, List);
is_string(_) -> false.
