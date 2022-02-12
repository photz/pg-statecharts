

create function jsonb_as_array(jsonb) returns jsonb language sql as $$
  select case when jsonb_typeof($1) = 'array' then $1 else jsonb_build_array($1) end
$$;

create function create_transition(
  machine int,
  path ltree,
  conf jsonb,
  out _transition_id int
)
returns int language plpgsql as $$
declare
  _target text := case
                    when jsonb_typeof(conf) = 'string' then conf #>> '{}'
                    else conf->>'target'
                  end;
  _internal bool;
  _target_id int;
  _meta jsonb;
begin
  
  if jsonb_typeof(conf) = 'string' then
    conf := '{}';
  end if;

  _meta := conf->'meta';

  if starts_with(_target, '.') then
     _internal := coalesce(conf->'internal', 'true');
    raise exception 'Not implemented';

  elsif starts_with(_target, '#') then

    _internal := false;

    select id
      into _target_id
      from state_node s
     where s.machine = $1
           and s.meta->>'scxml-id' = substring(_target from 2);

  else

    select id
      into _target_id
      from state_node s
     where subpath($2, 0, -1) || _target = s.path
           and s.machine = $1;

    if _target_id is null then
      raise  'No sibling found with key %', _target;
    end if;

  end if;

  insert into transition (target, internal)
  values (_target_id, _internal)
  returning id into _transition_id;

  if conf ? 'actions' then
    insert into transition_action (transition, action)
    select _transition_id, create_action(action_conf)
      from jsonb_array_elements(jsonb_as_array(conf->'actions')) _ (action_conf);
  end if;


  if conf ? 'cond' then
    perform add_guard(_transition_id, cond_conf)
    from jsonb_array_elements(jsonb_as_array(conf->'cond')) _ (cond_conf);
  end if;
  
end $$;

create function add_guard(
  transition int,
  conf jsonb,
  out _id int
) returns int language plpgsql as $$
declare
  _type condition_type := case
                            when jsonb_typeof(conf) = 'string' then conf #>> '{}'
                            else conf #>> '{type}'
                          end;

  _table_name text := _type || '_guard';
  _params jsonb;
  _id int;
  _has_table bool;
begin
  raise notice 'type is %', _type;

  insert into guard (transition, "type")
  values ($1, _type)
  returning id into _id;

  raise notice 'added guard: %', (select to_jsonb(guard.*) from guard where id = _id);

  if jsonb_typeof(conf) = 'string' then
    _params := jsonb_build_object();

  elsif jsonb_typeof(conf) = 'object' then
    _params := conf - 'type';
  else 
    raise exception 'Conditions must be specified as either a string or an object, got %', conf;
  end if;

  raise notice 'params are: %', _params;

  select exists (
    select
      from information_schema.tables
     where table_schema = 'public'
           and table_name = _table_name
  )
  into _has_table;

  if _has_table then
    perform insert_from_json(
      _table_name,
      jsonb_build_object('id', _id) || _params
    );

  elsif not (_params <@ '{}') then
    raise exception 'Condition type % does not accept parameters, but got %',
          _type, _params;
  end if;
end $$;


create function machine_add_state(
  machine int,
  parent_id int,
  path ltree,
  conf jsonb,
  out _state_id int
)
returns int language plpgsql as $$
declare
  _child_key text;
  _child_conf jsonb;
  _tags text[];
  _type state_node_type;
  _initial_state_node_id int;
  _meta jsonb := conf->'meta';
begin
  _state_id := nextval(pg_get_serial_sequence('state_node', 'id'));

  if jsonb_typeof(conf->'tags') = 'string' then
    _tags := array[conf->>'tags'];
  elsif jsonb_typeof(conf->'tags') = 'array' then
    _tags := (select array_agg(tag) from jsonb_array_elements_text(conf->'tags') _ (tag));
  end if;
  
  _type := case
             when conf ? 'type' then conf->>'type'
             when conf ? 'states' then 'compound'
             else 'atomic'
           end;


  if _type = 'compound' then

    if not conf ? 'initial' then
      raise exception 'State assumed to be compound state but "initial" property is missing';
    end if;

    if not (conf->'states') ? (conf->>'initial') then
      raise exception 'State not found: %', conf->>'initial';
    end if;

    for _child_key, _child_conf in (select key, value from jsonb_each(conf->'states')) loop

      if _child_key = conf->>'initial' then
        _initial_state_node_id := machine_add_state($1, _state_id, $3 || _child_key, _child_conf);
      else
        perform machine_add_state($1, _state_id, $3 || _child_key, _child_conf);
      end if;

    end loop;
    
  end if;

  if conf ? 'id' then
    _meta := jsonb_build_object('scxml-id', conf->>'id') || coalesce(_meta, '{}');
  end if;

  insert into state_node (id, machine, path, tags, "type", initial, meta, parent)
  values (_state_id, machine, path, _tags, _type, _initial_state_node_id, _meta, parent_id);
end $$;

create function create_invocation(
  state_id int,
  conf jsonb
)
returns int language plpgsql as $f$
declare
  _src jsonb;
  _onerror jsonb;
  _ondone jsonb;
  _id text;
  _type service_type;
  _invocation_id int;
  _external_service external_service_type;
  _external_service_params jsonb;
  _table_name text;
  _machine_id int;
  _path ltree;
begin
  select machine, path
    into _machine_id, _path
    from state_node
   where id = state_id;

  select id, src, "onDone", "onError"
    into _id, _src, _ondone, _onerror
    from jsonb_to_record(conf) _ (id text, src jsonb, "onDone" jsonb, "onError" jsonb);

  _type := 'external';

  _invocation_id := nextval(pg_get_serial_sequence('invocation', 'id'));

  if jsonb_typeof(_src) = 'string' then

    _external_service := _src #>> '{}';
    _external_service_params := jsonb_build_object();

  elsif jsonb_typeof(_src) = 'object' then

    _external_service := _src #>> '{type}';
    _external_service_params := _src - 'type';

  else
    raise exception 'Invocation must be specified as either an object or a string, got: %', _src;
  end if;

  insert into invocation (id, state, "type", machine, external_service)
  values (_invocation_id, state_id, _type, null, _external_service);

  _table_name := _external_service || '_service_invocation';

  if exists (
    select
      from information_schema.tables
     where table_schema = 'public'
           and table_name = _table_name
  ) then

   perform insert_from_json(
     _table_name,
     jsonb_build_object('id', _invocation_id) || _external_service_params
   );

  elsif not (_external_service_params <@ '{}') then

    raise exception 'External service % does not accept any parameters, got %',
      _external_service, _external_service_params;
  end if;


  if _ondone is not null then

    insert into invocation_ondone (invocation, transition, "order")
    select _invocation_id,
           create_transition(_machine_id, _path, transition_conf),
           row_number() over ()
      from jsonb_array_elements(jsonb_as_array(_ondone)) _ (transition_conf);
           
  end if;

  if _onerror is not null then

    insert into invocation_onerror (invocation, transition, "order")
    select _invocation_id,
           create_transition(_machine_id, _path, transition_conf),
           row_number() over()
      from jsonb_array_elements(jsonb_as_array(_onerror)) _ (transition_conf);

  end if;
  
  return _invocation_id;
end $f$;


create function machine_add_state2(
  machine int,
  path ltree,
  conf jsonb
)
returns void language plpgsql as $$
declare
  _child_key text;
  _child_conf jsonb;
  _state_id int;
  _initial int;
begin
  select id,
         initial
    into _state_id,
         _initial
    from state_node s
   where s.path = $2
         and s.machine = $1;

  if conf ? 'invoke' then
    perform create_invocation(_state_id, invoke_conf)
       from jsonb_array_elements(jsonb_as_array(conf->'invoke')) _ (invoke_conf);
  end if;

  if conf ? 'after' then
    insert into after (state, transition, duration)
    select _state_id,
           create_transition($1, $2, transition_conf),
           case
             when duration ~ '^\d+$' then make_interval(secs => duration::float / 1000.0)
             else duration::interval
           end
      from jsonb_each(conf->'after') _ (duration, transition_conf);
  end if;

  if conf ? 'always' then
    insert into eventless (state, transition, "order")
    select _state_id,
           create_transition($1, $2, transition_conf),
           row_number() over ()
      from jsonb_array_elements(conf->'always') _ (transition_conf);
  end if;

  if conf ? 'on' then
    insert into event_mapping (state, event_type, transition, "order")
    select _state_id,
           event_type::event_type,
           create_transition($1, $2, t.conf),
           row_number() over ()
      from jsonb_each(conf->'on') entries (event_type, transitions),
           jsonb_array_elements(jsonb_as_array(transitions)) t (conf);
  end if;

  if conf ? 'entry' then
    insert into onentry_action (state, action, "order")
    select _state_id,
           create_action(action_conf),
           row_number() over ()
      from jsonb_array_elements(jsonb_as_array(conf->'entry')) _ (action_conf);
  end if;

  if conf ? 'exit' then
    insert into onexit_action (state, action, "order")
    select _state_id,
           create_action(action_conf),
           row_number() over ()
      from jsonb_array_elements(jsonb_as_array(conf->'exit')) _ (action_conf);
  end if;

  if conf ? 'states' then
    perform machine_add_state2(machine, path || child_key, child_conf)
       from jsonb_each(conf->'states') _ (child_key, child_conf);
  end if;

  if conf ? 'onDone' then

    insert into state_ondone (state, transition, "order")
    select _state_id,
           create_transition($1, $2, transition_conf),
           row_number() over ()
      from jsonb_array_elements(jsonb_as_array(conf->'onDone')) _ (transition_conf);

  end if;

end $$;



create function create_action(
  conf jsonb,
  out _id int
) returns int language plpgsql as $$
declare
  _type action_type;
  _params jsonb;
  _tbl text;
  _has_table bool;
begin

  if jsonb_typeof(conf) = 'string' then

    _type := conf #>> '{}';
    _params := '{}';

  elsif jsonb_typeof(conf) = 'object' then
    
    _type := conf #>> '{type}';
    _params := conf - 'type';

  else
    raise exception 'Actions must be specified either as a string or an object, got %', conf;

  end if;

  _tbl := _type || '_action';

  insert into action ("type") values (_type) returning id into _id;
    
  select exists (select from information_schema.tables where table_name = _tbl)
    into _has_table;

  if _has_table then 
    perform insert_from_json(_tbl, jsonb_build_object('id', _id) || _params);

  elsif not (_params <@ '{}') then
    raise exception 'Actions of type % don''t accept parameters, got %', _type, _params;
  end if;

end $$;


create function create_machine(
  name text,
  "type" machine_type,
  conf jsonb,
  out _id int
)
returns int language plpgsql as $$
declare
  _machine_id int;
  _root_state_id int;
  _root_key ltree;
  _first_child_id int;
begin
  _machine_id := nextval(pg_get_serial_sequence('machine', 'id'));

  _root_key := 'c'::ltree || coalesce(conf->>'key', 'root')::ltree;

  _root_state_id := nextval(pg_get_serial_sequence('state_node', 'id'));

  _first_child_id := machine_add_state(_machine_id, _root_state_id, _root_key, conf);

  insert into state_node (id, "type", path, machine, initial)
  values (_root_state_id,
          'compound',
          'c',
          _machine_id,
          _first_child_id);

  perform machine_add_state2(_machine_id, _root_key, conf);

  insert into machine (id, "type", name, state)
  values (_machine_id, $2, name, _root_state_id)
  returning id into _id;
end $$;

