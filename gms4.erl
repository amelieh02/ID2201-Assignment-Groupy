-module(gms4).
-export([start/1, start/2]).

-define(timeout, 1000).
-define(arghh, 100).
-define(resend_interval, 500).
-define(drop, 50).

start(Id) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Rnd, Self) end)}.

start(Id, Grp) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Rnd, Self) end)}.

init(Id, Rnd, Master) ->
    random:seed(Rnd, Rnd, Rnd),
    io:format("gms4 ~w: I am the leader, pid ~w~n", [Id, self()]),
    erlang:send_after(?resend_interval, self(), resend_tick),
    leader(Id, Master, 0, [], [Master], []).

init(Id, Grp, Rnd, Master) ->
    random:seed(Rnd, Rnd, Rnd),
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, N, [Leader|Slaves], Group} ->
            erlang:monitor(process, Leader),
            Master ! {view, Group},
            slave(Id, Master, Leader, N+1, {view, N, [Leader|Slaves], Group}, Slaves, Group)
    after ?timeout ->
        Master ! {error, "no reply from leader"}
    end.

leader(Id, Master, N, Slaves, Group, Pending) ->
    receive
        {mcast, Msg} ->
            bcast(Id, {msg, N, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, N+1, Slaves, Group, [{N, {msg, N, Msg}, Slaves, []} | Pending]);

        {ack, From, I} ->
            Pending2 = ack_message(I, From, Pending),
            leader(Id, Master, N, Slaves, Group, Pending2);

        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast(Id, {view, N, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            leader(Id, Master, N+1, Slaves2, Group2, Pending);

        resend_tick ->
            Pending2 = resend_unacked(Id, Pending),
            case Pending2 of
                [] -> ok;
                _  -> io:format("leader ~w: ~w message(s) still pending~n", [Id, length(Pending2)])
            end,
            erlang:send_after(?resend_interval, self(), resend_tick),
            leader(Id, Master, N, Slaves, Group, Pending2);
        
        stop -> 
            ok
    end.

ack_message(N, From, Pending) ->
    lists:map(
        fun({Nr, Msg, Nodes, Acked}) when Nr == N ->
                {Nr, Msg, Nodes, lists:usort([From|Acked])};
           (Entry) ->
                Entry
        end, Pending).

resend_unacked(Id, Pending) ->
    lists:filter(
        fun({_N, Msg, Nodes, Acked}) ->
            Missing = Nodes -- Acked,
            case Missing of
                [] -> false;  
                _  -> 
                    bcast(Id, Msg, Missing), 
                    true  
            end
        end, Pending).

bcast(Id, Msg, Nodes) ->
    lists:foreach(fun(Node) -> maybe_send(Id, Msg, Node) end, Nodes).

crash(Id) ->
    case random:uniform(?arghh) of
        ?arghh ->
            io:format("leader ~w: crash~n", [Id]),
            exit(no_luck);
        _ ->
            ok
    end.

maybe_send(Id, Msg, Node) ->
    case random:uniform(?drop) of
        ?drop -> ok;
        _ ->
            Node ! Msg,
            crash(Id)
    end.

slave(Id, Master, Leader, N, Last, Slaves, Group) ->
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, N, Last, Slaves, Group);

        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, N, Last, Slaves, Group);

        {msg, I, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group);

        {msg, N, Msg} ->
            Master ! Msg,
            Leader ! {ack, self(), N},
            slave(Id, Master, Leader, N+1, {msg, N, Msg}, Slaves, Group);

        {view, I, _, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group);

        {view, N, [Leader|Slaves2], Group2} ->
            Master ! {view, Group2},
            slave(Id, Master, Leader, N+1, {view, N, [Leader|Slaves2], Group2}, Slaves2, Group2);

        {'DOWN', _Ref, process, Leader, _Reason} ->
            election(Id, Master, N, Last, Slaves, Group);

        stop ->
            ok
    end.

election(Id, Master, N, Last, Slaves, [_|Group]) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            io:format("gms4 ~w: I am elected leader~n", [Id]),
            bcast(Id, Last, Rest),
            Master ! {view, Group},
            erlang:send_after(?resend_interval, self(), resend_tick),
            leader(Id, Master, N, Rest, Group, []);
        [Leader|Rest] ->
            io:format("gms4 ~w: new leader is ~w~n", [Id, Leader]),
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, N, Last, Rest, Group)
    end.