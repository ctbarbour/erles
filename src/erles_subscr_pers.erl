-module(erles_subscr_pers).

-export([ack_events/2]).
-export([nak_events/3]).
-export([stop/1]).

-export([init/1]).
-export([handle_sync_event/4]).
-export([handle_event/3]).
-export([handle_info/3]).
-export([code_change/4]).
-export([terminate/3]).

-export([disconnected/2, disconnected/3]).
-export([pending/2, pending/3]).
-export([retry_pending/2, retry_pending/3]).
-export([subscribed/2, subscribed/3]).

-include("erles_clientapi_pb.hrl").
-include("erles.hrl").
-include("erles_internal.hrl").

-record(state, {corr_id,
                els_pid,
                conn_pid,
                reply_pid,
                timeout,
                retries,
                retry_delay,
                auth,
                timer_ref,
                group_id,
                stream_id,
                sub_id,
                sub_kind,
                resolve_links,
                sub_pid,
                sub_mon_ref}).

ack_events(Pid, EventIds)
  when is_list(EventIds) ->
    gen_fsm:send_event(Pid, {ack_events, EventIds}).

nak_events(Pid, Action, EventIds)
  when is_list(EventIds) ->
    gen_fsm:send_event(Pid, {nak_events, Action, EventIds}).

stop(Pid) ->
    gen_fsm:sync_send_event(Pid, stop).


init({connect_to_persistent_subscription, S=#sys_params{}, {GroupId, StreamId, SubPid}}) ->
    process_flag(trap_exit, true),
    MonRef = erlang:monitor(process, SubPid),
    State = #state{corr_id = S#sys_params.corr_id,
                   els_pid = S#sys_params.els_pid,
                   conn_pid = S#sys_params.conn_pid,
                   reply_pid = S#sys_params.reply_pid,
                   timeout = S#sys_params.op_timeout,
                   retries = S#sys_params.op_retries,
                   retry_delay = S#sys_params.retry_delay,
                   auth = S#sys_params.auth,
                   timer_ref = none,
                   group_id = GroupId,
                   stream_id = case StreamId of
                                   all -> <<>>;
                                   _   -> StreamId
                               end,
                   sub_kind = case StreamId of
                                  all -> all;
                                  _   -> stream
                              end,
                   resolve_links = true,
                   sub_pid = SubPid,
                   sub_mon_ref = MonRef},
    {ok, disconnected, State}.


handle_event(Event, StateName, State) ->
    io:format("Unexpected GLOBAL ASYNC EVENT  ~p in state ~p, state data ~p~n", [Event, StateName, State]),
    {next_state, StateName, State}.

handle_sync_event(Event, From, StateName, State) ->
    io:format("Unexpected GLOBAL SYNC EVENT ~p from ~p in state ~p, state data ~p~n", [Event, From, StateName, State]),
    {next_state, StateName, State}.


disconnected(connected, State) ->
    issue_subscribe_request(State);

disconnected({aborted, Reason}, State) ->
    abort(State, {error, {aborted, Reason}});

disconnected({subscriber_down, Reason}, State) ->
    complete(State, {error, {subscriber_down, Reason}});

disconnected(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, disconnected, State]),
    {next_state, disconnected, State}.

disconnected(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, disconnected, State]),
    {next_state, disconnected, State}.


pending({pkg, Cmd, CorrId, _Auth, Data}, State=#state{corr_id=CorrId}) ->
    case Cmd of
        persistent_subscription_confirmation ->
            Dto = erles_clientapi_pb:decode_msg(Data, 'PersistentSubscriptionConfirmation'),
            SubPos = case State#state.sub_kind of
                         all    -> Dto#'PersistentSubscriptionConfirmation'.last_commit_position;
                         stream -> Dto#'PersistentSubscriptionConfirmation'.last_event_number
                     end,
            gen_fsm:reply(State#state.reply_pid, {ok, self(), SubPos}),
            NewState = succeed(State),
            {next_state, subscribed, NewState#state{sub_id = Dto#'PersistentSubscriptionConfirmation'.subscription_id}};
        subscription_dropped ->
            Dto = erles_clientapi_pb:decode_msg(Data, 'SubscriptionDropped'),
            Reason = drop_reason(Dto#'SubscriptionDropped'.reason),
            complete(State, {error, Reason});
        not_handled ->
            not_handled(Data, State);
        not_authenticated ->
            complete(State, {error, {not_authenticated, Data}});
        bad_request ->
            complete(State, {error, {bad_request, Data}});
        _ ->
            io:format("Unexpected command received: ~p, data: ~p.~n", [Cmd, Data]),
            {next_state, pending, State}
    end;

pending({timeout, TimerRef, timeout}, State=#state{timer_ref=TimerRef}) ->
    complete(State, {error, server_timeout});

pending(disconnected, State) ->
    Retries = State#state.retries - 1,
    case Retries >= 0 of
        true ->
            cancel_timer(State#state.timer_ref),
            {next_state, disconnected, State#state{retries=Retries, timer_ref=none}};
        false ->
            complete(State, {error, retry_limit})
    end;

pending({aborted, Reason}, State) ->
    abort(State, {error, {aborted, Reason}});

pending({subscriber_down, Reason}, State=#state{}) ->
    complete(State, {error, {subscriber_down, Reason}});

pending(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, pending, State]),
    {next_state, pending, State}.

pending(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, pending, State]),
    {next_state, pending, State}.


retry_pending({timeout, TimerRef, retry}, State=#state{timer_ref=TimerRef}) ->
    issue_subscribe_request(State);

retry_pending(disconnected, State) ->
    cancel_timer(State#state.timer_ref),
    {next_state, disconnected, State#state{timer_ref=none}};

retry_pending({aborted, Reason}, State) ->
    abort(State, {error, {aborted, Reason}});

retry_pending({subscriber_down, Reason}, State=#state{}) ->
    complete(State, {error, {subscriber_down, Reason}});

retry_pending(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, retry_pending, State]),
    {next_state, retry_pending, State}.

retry_pending(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, retry_pending, State]),
    {next_state, retry, State}.


subscribed({pkg, Cmd, CorrId, _Auth, Data}, State=#state{corr_id=CorrId}) ->
    case Cmd of
        persistent_subscription_stream_event_appeared ->
            Dto = erles_clientapi_pb:decode_msg(Data, 'PersistentSubscriptionStreamEventAppeared'),
            {E, P} = erles_utils:resolved_event(
                       State#state.sub_kind,
                       Dto#'PersistentSubscriptionStreamEventAppeared'.event),
            notify(State, {subscr_event, self(), E, P}),
            {next_state, subscribed, State};
        subscription_dropped ->
            Dto = erles_clientapi_pb:decode_msg(Data, 'SubscriptionDropped'),
            Reason = Dto#'SubscriptionDropped'.reason,
            notify(State, {unsubscribed, self(), Reason}),
            complete(State);
        _ ->
            io:format("Unexpected command received: ~p, data: ~p.~n", [Cmd, Data]),
            {next_state, pending, State}
    end;

subscribed({ack_events, EventIds}, State=#state{}) ->
    Dto = #'PersistentSubscriptionAckEvents'{
             subscription_id = State#state.sub_id,
             processed_event_ids = EventIds
            },
    Bin = erles_clientapi_pb:encode_msg(Dto),
    Pkg = erles_pkg:create(persistent_subscription_ack_events, State#state.corr_id, State#state.auth, Bin),
    erles_conn:send(State#state.conn_pid, Pkg),
    {next_state, subscribed, State};

subscribed({nak_events, Action, EventIds}, State=#state{}) ->
    Dto = #'PersistentSubscriptionNakEvents'{
             subscription_id = State#state.sub_id,
             processed_event_ids = EventIds,
             action = Action
            },
    Bin = erles_clientapi_pb:encode_msg(Dto),
    Pkg = erles_pkg:create(persistent_subscription_nak_events, State#state.corr_id, State#state.auth, Bin),
    erles_conn:send(State#state.conn_pid, Pkg),
    {next_state, subscribed, State};

subscribed(disconnected, State=#state{}) ->
    notify(State, {unsubscribed, self(), connection_dropped}),
    complete(State);

subscribed({aborted, Reason}, State=#state{}) ->
    notify(State, {unsubscribed, self(), {aborted, Reason}}),
    complete(State);

subscribed({subscriber_down, Reason}, State=#state{}) ->
    notify(State, {unsubscribed, self(), {subscriber_down, Reason}}),
    complete(State);

subscribed(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, subscribed, State]),
    {next_state, retry_pending, State}.

subscribed(stop, From, State) ->
    issue_unsubscribe_request(State),
    notify(State, {unsubscribed, self(), requested_by_client}),
    gen_fsm:reply(From, ok),
    complete(State);

subscribed(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, subscribed, State]),
    {next_state, retry, State}.


handle_info(TimerMsg={timeout, _, _}, StateName, State) ->
    gen_fsm:send_event(self(), TimerMsg),
    {next_state, StateName, State};

handle_info({'DOWN', MonRef, process, SubPid, Reason}, StateName, State=#state{sub_pid=SubPid, sub_mon_ref=MonRef}) ->
    gen_fsm:send_event(self(), {subscriber_down, Reason}),
    {next_state, StateName, State};

handle_info(Msg, StateName, State) ->
    io:format("Unexpected INFO message ~p, state name ~p, state data ~p~n", [Msg, StateName, State]),
    {next_state, StateName, State}.


terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


issue_subscribe_request(State=#state{}) ->
    Dto = #'ConnectToPersistentSubscription'{
             subscription_id = State#state.group_id,
             event_stream_id = State#state.stream_id,
             allowed_in_flight_messages = 1
            },
    Bin = erles_clientapi_pb:encode_msg(Dto),
    Pkg = erles_pkg:create(connect_to_persistent_subscription, State#state.corr_id, State#state.auth, Bin),
    erles_conn:send(State#state.conn_pid, Pkg),
    TimerRef = erlang:start_timer(State#state.timeout, self(), timeout),
    {next_state, pending, State#state{timer_ref=TimerRef}}.

issue_unsubscribe_request(State) ->
    Dto = #'UnsubscribeFromStream'{},
    Bin = erles_clientapi_pb:encode_msg(Dto),
    Pkg = erles_pkg:create(unsubscribe_from_stream, State#state.corr_id, State#state.auth, Bin),
    erles_conn:send(State#state.conn_pid, Pkg).

complete(State, Result) ->
    cancel_timer(State#state.timer_ref),
    gen_fsm:reply(State#state.reply_pid, Result),
    erles_fsm:operation_completed(State#state.els_pid, State#state.corr_id),
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    {stop, normal, State}.

complete(State) ->
    cancel_timer(State#state.timer_ref),
    erles_fsm:operation_completed(State#state.els_pid, State#state.corr_id),
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    {stop, normal, State}.

succeed(State) ->
    cancel_timer(State#state.timer_ref),
    State#state{timer_ref=none}.

abort(State, Result) ->
    cancel_timer(State#state.timer_ref),
    gen_fsm:reply(State#state.reply_pid, Result),
    {stop, normal, State}.

notify(State, Msg) ->
    State#state.sub_pid ! Msg.

not_handled(Data, State) ->
    Dto = erles_clientapi_pb:decode_msg(Data, 'NotHandled'),
    case Dto#'NotHandled'.reason of
        'NotMaster' ->
            MasterInfo = erles_clientapi_pb:decode_msg(Dto#'NotHandled'.additional_info, 'NotHandled.MasterInfo'),
            Ip = erles_utils:parse_ip(MasterInfo#'NotHandled.MasterInfo'.external_tcp_address),
            Port = MasterInfo#'NotHandled.MasterInfo'.external_tcp_port,
            cancel_timer(State#state.timer_ref),
            case erles_conn:reconnect(State#state.conn_pid, Ip, Port) of
                already_connected ->
                    issue_subscribe_request(State);
                ok ->
                    {next_state, disconnected, State#state{timer_ref=none}}
            end;
        OtherReason ->
            retry(OtherReason, State)
    end.

retry(_Reason, State=#state{retries=Retries}) when Retries > 0 ->
                                                %io:format("Retrying subscription because ~p.~n", [_Reason]),
    cancel_timer(State#state.timer_ref),
    NewCorrId = erles_utils:gen_uuid(),
    erles_fsm:operation_restarted(State#state.els_pid, State#state.corr_id, NewCorrId),
    TimerRef = erlang:start_timer(State#state.retry_delay, self(), retry),
    {next_state, retry_pending, State#state{corr_id=NewCorrId, retries=Retries-1, timer_ref=TimerRef}};

retry(_Reason, State=#state{retries=Retries}) when Retries =< 0 ->
                                                %io:format("Retrying subscription because ~p... Retries limit reached!~n", [_Reason]),
    complete(State, {error, retry_limit}).

cancel_timer(none)     -> ok;
cancel_timer(TimerRef) -> erlang:cancel_timer(TimerRef).

drop_reason('AccessDenied') -> access_denied;
drop_reason('Unsubscribed') -> unsubscribed;
drop_reason(Reason)         -> Reason.
