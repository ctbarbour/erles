-module(erles_subscr_perm).
-behavior(gen_server).

-export([start_link/7, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erles_internal.hrl").

-record(state, {worker_pid,
                sub_pid,
                sub_mon_ref}).

-record(worker, {perm_sub_pid,
                 els_pid,
                 sub_pid,
                 stream_id,
                 read_batch,
                 opts}).

start_link(ElsPid, StreamId, From, SubPid, Auth, ResolveLinks, ReadBatch) ->
    InitParams = {ElsPid, StreamId, From, SubPid, Auth, ResolveLinks, ReadBatch},
    gen_server:start_link(?MODULE, InitParams, []).

stop(Pid) ->
    gen_server:call(Pid, stop).


init({ElsPid, StreamId, From, SubPid, Auth, ResolveLinks, ReadBatch}) ->
    process_flag(trap_exit, true),
    MonRef = erlang:monitor(process, SubPid),
    WorkerState = #worker{perm_sub_pid=self(),
                          els_pid=ElsPid,
                          sub_pid=SubPid,
                          stream_id=StreamId,
                          read_batch=ReadBatch,
                          opts=[{resolve, ResolveLinks}, {auth, Auth}]},
    WorkerPid = spawn_link(fun() ->
        case From of
            live -> subscribe(WorkerState, live);
            _    -> catchup(WorkerState, From)
        end
    end),
    State = #state{worker_pid=WorkerPid,
                   sub_pid=SubPid,
                   sub_mon_ref=MonRef},
    {ok, State}.


handle_call(stop, _From, State) ->
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    State#state.sub_pid ! {unsubscribed, self(), requested_by_client},
    State#state.worker_pid ! stop,
    {stop, normal, ok, State};

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p~n State: ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast({worker_stopped, Reason}, State) ->
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    State#state.sub_pid ! {unsubscribed, self(), Reason},
    {stop, normal, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p~n State: ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({'EXIT', WorkerPid, Reason}, State=#state{worker_pid=WorkerPid}) ->
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    State#state.sub_pid ! {unsubscribed, self(), {worker_failed, Reason}},
    {stop, normal, State};

handle_info({'DOWN', MonRef, process, SubPid, _Reason}, State=#state{sub_pid=SubPid, sub_mon_ref=MonRef}) ->
    State#state.worker_pid ! stop,
    {stop, normal, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p~nState: ~p~n", [Msg, State]),
    {noreply, State}.


terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


catchup(State, Pos={_PosKind, StreamPos}) ->
    receive stop -> ok
    after 0 ->
        ElsPid = State#worker.els_pid,
        StreamId = State#worker.stream_id,
        ReadBatch = State#worker.read_batch,
        Opts = State#worker.opts,
        case erles:read_stream(ElsPid, StreamId, StreamPos, ReadBatch, forward, Opts) of
            {ok, Events, NextPos, false} ->
                send_events(State#worker.sub_pid, Events, Pos),
                catchup(State, {inclusive, NextPos});
            {ok, Events, NextPos, true} ->
                send_events(State#worker.sub_pid, Events, Pos),
                subscribe(State, {inclusive, NextPos});
            {error, Reason} ->
                stop(State#worker.perm_sub_pid, {read_failed, Reason})
        end
    end.

subscribe(State, Pos) ->
    receive stop -> ok
    after 0 ->
        ElsPid = State#worker.els_pid,
        StreamId = State#worker.stream_id,
        Opts = State#worker.opts,
        case erles:subscribe_prim(ElsPid, StreamId, Opts) of
            {ok, SubPid, SubPos} ->
                case Pos of
                    live -> live(State, {inclusive, SubPos}, SubPid);
                    _    -> switchover(State, Pos, SubPid)
                end;
            {error, Reason} ->
                stop(State#worker.perm_sub_pid, {subscription_failed, Reason})
        end
    end.

switchover(State, Pos={_PosKind, StreamPos}, SubscrPid) ->
    receive stop -> erles:unsubscribe_prim(SubscrPid)
    after 0 ->
        ElsPid = State#worker.els_pid,
        StreamId = State#worker.stream_id,
        ReadBatch = State#worker.read_batch,
        Opts = State#worker.opts,
        case erles:read_stream(ElsPid, StreamId, StreamPos, ReadBatch, forward, Opts) of
            {ok, Events, NextPos, false} ->
                send_events(State#worker.sub_pid, Events, Pos),
                switchover(State, {inclusive, NextPos}, SubscrPid);
            {ok, Events, NextPos, true} ->
                send_events(State#worker.sub_pid, Events, Pos),
                live(State, {inclusive, NextPos}, SubscrPid);
            {error, Reason} ->
                erles:unsubscribe_prim(SubscrPid),
                stop(State#worker.perm_sub_pid, {read_failed, Reason})
        end
    end.

live(State, Pos, SubscrPid) ->
    receive
        stop ->
            erles:unsubscribe_prim(SubscrPid);
        {unsubscribed, SubscrPid, {aborted, _}} ->
            stop(State#worker.perm_sub_pid, subscription_aborted);
        {unsubscribed, SubscrPid, _Reason} ->
            catchup(State, Pos);
        {subscr_event, SubscrPid, Event, NewPos} ->
            send_event(State#worker.sub_pid, Event, NewPos, Pos),
            live(State, {exclusive, NewPos}, SubscrPid);
        Unexpected ->
            io:format("Unexpected event in live phase of perm_subscr: ~p.~n", [Unexpected])
    end.

stop(PermSubPid, Reason) -> PermSubPid ! {worker_stopped, self(), Reason}.

send_events(Pid, Events, Pos) when is_list(Events) ->
    lists:foreach(fun({E, P}) -> send_event(Pid, E, P, Pos) end, Events).

send_event(Pid, Event, EventPos, {inclusive, SubPos}) when EventPos >= SubPos ->
    Pid ! {subscr_event, self(), Event, EventPos};

send_event(Pid, Event, EventPos, {exclusive, SubPos}) when EventPos > SubPos ->
    Pid ! {subscr_event, self(), Event, EventPos};

send_event(_Pid, _Event, _EventPos, {_PosKind, _SubPos}) ->
    ok.

