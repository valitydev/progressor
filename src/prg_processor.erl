-module(prg_processor).

-behaviour(gen_server).

-include("progressor.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export([process/4]).

-define(SERVER, ?MODULE).

-record(prg_processor_state, {}).

%% API

process(Pid, HandlerOpts, Request, Timeout) ->
    %% let it crash if timeout. worker will be restarted by supervisor
    %% maybe try/catch ???
    gen_server:call(Pid, {HandlerOpts, Request}, Timeout).
%    ReqId = gen_server:send_request(Pid, {HandlerOpts, Request}),
%    case gen_server:wait_response(ReqId, Timeout) of
%        timeout ->
%            {error, timeout};
%        {reply, Result} ->
%            Result
%    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #prg_processor_state{}}.

handle_call({#{client := Handler, options := Options}, Request}, _From, State = #prg_processor_state{}) ->
    Response =
        try Handler:process(Request, Options) of
            {ok, _Result} = OK -> OK;
            {error, _Reason} = ERR -> ERR;
            _Unsupported -> {error, <<"unsupported_result">>}
        catch
            Class:Term ->
                {error, {exception, Class, Term}}
        end,
    {reply, Response, State}.

handle_cast(_Request, State = #prg_processor_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #prg_processor_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #prg_processor_state{}) ->
    ok.

code_change(_OldVsn, State = #prg_processor_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
