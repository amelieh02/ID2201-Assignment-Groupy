-module(test).
-compile(export_all).

first(N, Module, Sleep) ->
    worker:start(N, Module, random:uniform(256), Sleep).

add(N, Module, Wrk, Sleep) ->
    worker:start(N, Module, random:uniform(256), Wrk, Sleep).

more(N, Module, Sleep) when N > 1 ->
    Wrk =first(1, Module, Sleep),
    Ns = lists:seq(2,N),
    lists:map(fun(Id) -> add(Id, Module, Wrk, Sleep) end, Ns),
    Wrk.

freeze(Wrk) ->
    Wrk ! {send, freeze}.

go(Wrk) ->
    Wrk ! {send, go}.

sleep(Wrk, Sleep) ->
    Wrk ! {send, {sleep, Sleep}}.

stop(Wrk) ->
    Wrk ! {send, stop}.