
-module(node3).

-define(Stabilize, 1000).
-define(Timeout, 10000).

%% API
-export([node/5, start/1, start/2]).


start(Id) ->
  start(Id, nil).

start(Id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
  Predecessor = nil,
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  % nil as the initial value
  node(Id, Predecessor, Successor, storage:create(), nil).

connect(Id, nil) ->
  % No need to monitor ourselves,
  {ok, {Id, monitor(self()), self()}};
connect(Id, Peer) ->
 % connecting to an existing ring
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      {ok, {Skey, monitor(Peer), Peer}}
  after ?Timeout ->
    io:format("Time out: no response from the peer~n",[])
  end.

% Ref is a reference produced by the monitor procedure.
node(Id, Predecessor, Successor, Store, Next) ->
  receive
    {key, Qref, Peer} ->
      % A peer needs to know our key
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor, Store, Next);
    {notify, New} ->
      % A new node informs us of its existence
      % i.e. suggesting that it might be our predecessor
      {Pred, NewStore} = notify(New, Id, Predecessor, Store),
      node(Id, Pred, Successor, NewStore, Next);
    {request, Peer} ->
      % A predecessor needs to know our predecessor, and our successor
      request(Peer, Predecessor, Successor),
      node(Id, Predecessor, Successor, Store, Next);
    {status, Pred, Nx} ->
      % Our successor informs us about its predecessor, and its successor
      {Succ, Nxt} = stabilize(Pred, Nx, Id, Successor),
      node(Id, Predecessor, Succ, Store, Nxt);
    stabilize ->
      % Send a request message to our successor.
      stabilize(Successor),
      node(Id, Predecessor, Successor, Store, Next);
    probe ->
      % sending probe message for traversal
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor, Store, Next);
    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor, Store, Next);
    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor, Store, Next);
    {add, Key, Value, Qref, Client} ->
      % Add key and value to store
      Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added, Next);
    {lookup, Key, Qref, Client} ->
      % Lookup key in store
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Store, Next);
    {handover, Elements} ->
      % Received a batch of elements to add to our Store
      % Message from a node that has accepted us as their predecessor
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged, Next);
    {'DOWN', Ref, process, _, _} ->
      {Pred, Succ, Nxt} = down(Ref, Predecessor, Successor, Next),
      node(Id, Pred, Succ, Store, Nxt);
    state ->
      io:format("ID: ~w~n", [Id]),
      io:format("Predecessor: ~p, Successor: ~p~n", [Predecessor, Successor]),
      io:format("Store: ~p~n", [Store]),
      node(Id, Predecessor, Successor, Store, Next);
    stop ->
      ok
  end.


down(Ref, {_, Ref, _}, Successor, Next) ->
  %  Predecessor died
  % Nothing to do, just set it as nil, will get another predecessor eventually
  {nil, Successor, Next};
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
  %  successor died
  % Adopt  Next node as  successor
  % Monitor the new successor
  % Run stabilization procedure again
  Nref = monitor(Npid),
  self() ! stabilize,
  {Predecessor, {Nkey, Nref, Npid}, nil}.

add(Key, Value, Qref, Client, Id, {Pkey, _, _}, {_, _, Spid}, Store) ->
  % Can use the key:between function
  case key:between(Key, Pkey, Id) of
    true ->
      % I should take care of this key
      Client ! {Qref, ok},
      NewStore = storage:add(Key, Value, Store),
      NewStore;
    false ->
      % We should not take care of this key, forward this add message to the successor
      Spid ! {add, Key, Value, Qref, Client},
      Store
  end.


lookup(Key, Qref, Client, Id, {Pkey, _, _}, Successor, Store) ->
  case key:between(Key, Pkey, Id) of
    true ->
      % This Key is our responsibility
      Result = storage:lookup(Key, Store),
      Client ! {Qref, Result};
    false ->
      % This Key is forwarded to successor
      {_, _, Spid} = Successor,
      Spid ! {lookup, Key, Qref, Client}
  end.


create_probe(Id, Successor) ->
  {_, _, Spid} = Successor,
  Spid ! {probe, Id, [{Id, self()}], erlang:system_time(micro_seconds)}.


remove_probe(T, Nodes) ->
  CurrentTime = erlang:system_time(micro_seconds),
  io:format("[~p] Received my own probe - Created time: ~p - Current time: ~p - Nodes: ~p~n",
    [self(), T, CurrentTime, Nodes]).


forward_probe(Ref, T, Nodes, Id, Successor) ->
  {_, _, Spid} = Successor,
  Spid ! {probe, Ref, lists:append(Nodes, [{Id, self()}]), T}.

stabilize(Pred, Nx, Id, Successor) ->
  {Skey, Sref, Spid} = Successor,
  case Pred of
    nil ->

      Spid ! {notify, {Id, self()}},
      {Successor, Nx};
    {Id, _} ->

      {Successor, Nx};
    {Skey, _} ->

      Spid ! {notify, {Id, self()}},
      {Successor, Nx};
    {Xkey, Xpid} ->

      case key:between(Xkey, Id, Skey) of
        true ->
          % We have a new successor, monitor the new one
          drop(Sref),
          NewSuccessor = {Xkey, monitor(Xpid), Xpid},
          NewNext = {Skey, Spid},
          self() ! stabilize,
          {NewSuccessor, NewNext};
        false ->
          Spid ! {notify, {Id, self()}},
          {Successor, Nx}
      end
  end.


schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).

% Send a request message to a PID.
stabilize({_, _, Spid}) ->
  Spid ! {request, self()}.

request(Peer, Predecessor, Successor) ->
  {Skey, _, Spid} = Successor,
  case Predecessor of
    nil ->
      Peer ! {status, nil, {Skey, Spid}};
    {Pkey, _, Ppid} ->
      Peer ! {status, {Pkey, Ppid}, {Skey, Spid}}
  end.


notify({Nkey, Npid}, Id, Predecessor, Store) ->
  case Predecessor of
    nil ->

      Keep = handover(Id, Store, Nkey, Npid),
      {{Nkey, monitor(Npid), Npid}, Keep};
    {Pkey, Pref, _} ->
      case key:between(Nkey, Pkey, Id) of
        true ->
          drop(Pref),
          Keep = handover(Id, Store, Nkey, Npid),
          {{Nkey,  monitor(Npid), Npid}, Keep};
        false ->
          {Predecessor, Store}
      end
  end.

% Split our store based on Nkey
handover(Id, Store, Nkey, Npid) ->
  {Rest, Keep} = storage:split(Id, Nkey, Store),
  Npid ! {handover, Rest},
  Keep.

monitor(Pid) ->
  erlang:monitor(process, Pid).

% NOTE: here the param is the ref and not the pid as mentioned in the hw doc.
drop(nil) ->
  ok;
drop(Ref) ->
  erlang:demonitor(Ref, [flush]).
