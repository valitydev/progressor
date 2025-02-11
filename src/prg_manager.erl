-module(prg_manager).

-include("progressor.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([child_spec/1]).
-export([create_namespace/2, get_namespace/1, destroy_namespace/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?MODULE).

-record(state, {
    namespaces = #{} :: #{namespace_id() => {reference(), pid(), namespace_opts()}},
    monitors = #{} :: #{{reference(), pid()} => namespace_id()}
}).

-type state() :: #state{}.

%% API

child_spec(ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, []},
        type => worker
    }.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_namespace(ID) ->
    gen_server:call(?SERVER, {get_namespace, ID}).

create_namespace(ID, Namespace) ->
    gen_server:call(?SERVER, {create_namespace, ID, Namespace}).

destroy_namespace(ID) ->
    gen_server:call(?SERVER, {destroy_namespace, ID}).

%% Callbacks

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #state{}}.

handle_call({create_namespace, ID, Namespace}, _From, State) ->
    {ok, Pid} = prg_namespaces_root_sup:create_namespace(ID, Namespace),
    {ok, State1} = register_namespace(ID, Namespace, Pid, State),
    {reply, ok, State1};
handle_call({get_namespace, ID}, _From, State) ->
    case get_namespace(ID, State) of
        {ok, {_Ref, _Pid, Namespace}} ->
            {reply, {ok, Namespace}, State};
        Ret ->
            {reply, Ret, State}
    end;
handle_call({destroy_namespace, ID}, _From, State) ->
    case unregister_namespace(ID, State) of
        {error, notfound} ->
            {reply, ok, State};
        {ok, Pid, State1} ->
            ok = prg_namespaces_root_sup:destroy_namespace(Pid),
            {reply, ok, State1}
    end;
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case unregister_if_monitored(Ref, Pid, State) of
        {ok, _Pid, State1} ->
            {noreply, State1};
        {error, notfound} ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info ~p", [Info]),
    {noreply, State}.

%%

register_namespace(ID, Namespace, Pid, State) ->
    Ref = erlang:monitor(process, Pid),
    {ok, State#state{
        namespaces = maps:put(ID, {Ref, Pid, Namespace}, State#state.namespaces),
        monitors = maps:put({Ref, Pid}, ID, State#state.monitors)
    }}.

unregister_if_monitored(Ref, Pid, State) ->
    case maps:get({Ref, Pid}, State#state.monitors, undefined) of
        undefined ->
            {error, notfound};
        ID ->
            unregister_namespace(ID, State)
    end.

unregister_namespace(ID, State) ->
    case get_namespace(ID, State) of
        {ok, {Ref, Pid, _}} ->
            true = erlang:demonitor(Ref),
            {ok, Pid, State#state{
                namespaces = maps:remove(ID, State#state.namespaces),
                monitors = maps:remove({Ref, Pid}, State#state.monitors)
            }};
        Ret ->
            Ret
    end.

get_namespace(ID, State) ->
    case maps:get(ID, State#state.namespaces, undefined) of
        undefined -> {error, notfound};
        {_Ref, _Pid, _Namespace} = V -> {ok, V}
    end.
