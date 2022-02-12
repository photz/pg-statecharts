alter type action_type add value 'no_params_test';

alter type action_type add value 'with_params_test';

create table with_params_test_action (
  id int references action primary key,
  string_param text not null,
  int_param int not null,
  json_param jsonb not null
);

alter type machine_type add value 'test';

alter type external_service_type add value 'no_params_test';

alter type external_service_type add value 'with_params_test';

create table with_params_test_service_invocation (
  id int references invocation primary key,
  string_param text not null,
  int_param int not null,
  json_param jsonb not null
);

alter type condition_type add value 'cond_without_param';
alter type condition_type add value 'cond_without_param2';

alter type condition_type add value 'cond_with_params';

create table cond_with_params_guard (
  id int references guard primary key,
  string_param text not null,
  int_param int not null,
  json_param jsonb not null
);

alter type event_type add value 'test';


set search_path = public, utils, ltree, from_json;

begin;

select * from no_plan();



create function test_empty_event_mapping() returns setof text language sql as $f$
  select lives_ok(
    $$
    select create_machine('test20', 'test', '{
             "initial": "a",
             "states": {
               "a": {
                 "on": {}
               }
             }
           }')
    $$
  )
$f$;

create function test_event() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test21', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "on": {
          "test": "b"
        }
      },
      "b": {}
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select event_mapping.event_type,
             target.path
        from event_mapping
             join transition
                  on transition.id = event_mapping.transition
             join state_node target
                  on target.id = transition.target
       where exists (select from state_node where id = event_mapping.state and machine = %s)
      $$,
      _machine_id
    ),
    $$ values ('test'::event_type, 'c.root.b'::ltree) $$
  );
end $f$;

create function test_invalid_event_type() returns setof text language sql as $f$
  select throws_ok(
    $$
    select create_machine('test22', 'test', '{
             "initial": "a",
             "states": {
               "a": {
                 "on": {
                   "invalid": "b"
                 }
               },
               "b": {}
             }
           }')
    $$,
    'invalid input value for enum event_type: "invalid"'
  )
$f$;

create function test_cond_string() returns setof text language plpgsql as $$
declare
  _machine_id int := create_machine('test', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          {
            "target": "b",
            "cond": "cond_without_param"
          }
        ]
      },
      "b": {}
    }
  }'::jsonb);
begin
  return query
  select is(g.type, 'cond_without_param')
    from state_node s
         join eventless e
              on e.state = s.id
         join transition t
              on e.transition = t.id
         join guard g
              on g.transition = t.id
   where s.machine = _machine_id;
end $$;

create function test_cond_array_of_strings() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test2', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          {
            "target": "b",
            "cond": ["cond_without_param", "cond_without_param2"]
          }
        ]
      },
      "b": {}
    }
  }');
begin
  return query
  select bag_eq(
           format($$
             select g.type
               from state_node s
                    join eventless e
                         on e.state = s.id
                    join transition t
                         on t.id = e.transition
                    join guard g
                         on g.transition = t.id
              where s.machine = %s
             $$,
             _machine_id
           ),
           array['cond_without_param', 'cond_without_param2']::condition_type[]
  );
end $f$;


create function test_cond_as_object()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test15', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          {
            "target": "b",
            "cond": {
              "type": "cond_without_param"
            }
          }
        ]
      },
      "b": {}
    }
  }');
begin
  return query select set_eq(
    format(
      $$
      select guard."type"
        from eventless
             join transition
                  on eventless.transition = transition.id
             join guard
                  on guard.transition = transition.id
       where exists (select from state_node where machine = %s and id = eventless.state)
      $$,
      _machine_id
    ),
    $$ values ('cond_without_param'::condition_type) $$
  );
end $f$;

create function test_cond_as_object_with_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test16', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          { 
            "target": "b",
            "cond": {
              "type": "cond_with_params",
              "string_param": "foo",
              "int_param": 42,
              "json_param": { "some": { "nested": "object" } }
            }
          }
        ]
      },
      "b": {}
    }
  }');
begin
  return query select set_eq(
    format(
      $$
      select guard."type",
             string_param,
             int_param,
             json_param
        from eventless
             join transition
                  on eventless.transition = transition.id
             join guard
                  on guard.transition = transition.id
             join cond_with_params_guard
                  on guard.id = cond_with_params_guard.id
       where exists (select from state_node where machine = %s and id = eventless.state)
      $$,
      _machine_id
    ),
    $$
    values ('cond_with_params'::condition_type,
            'foo',
            42,
            '{ "some": { "nested": "object" } }'::jsonb)
    $$
  );
end $f$;

create function test_cond_with_invalid_extra_params()
returns setof text language sql as $f$
  select throws_ok(
    $$
    select create_machine('test17', 'test', '{
             "initial": "a",
             "states": {
               "a": {
                 "always": [
                   {
                     "target": "b",
                     "cond": {
                       "type": "cond_without_param",
                       "invalid": "I am superfluous"
                     }
                   }
                 ]
               },
               "b": {}
             }
           }')
    $$,
    'Condition type cond_without_param does not accept parameters, but got {"invalid": "I am superfluous"}'
  );
$f$;

create function test_transition_as_string() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test3', 'test', '{
    "key": "root",
    "initial": "a",
    "states": {
      "a": {
        "always": ["b"]
      },
      "b": {}
    }
  }');
begin
  return query
  select bag_eq(
           format($$
             select text(source.path) as source_path,
                    text(target.path) as target_path
               from state_node source
                    join eventless e
                         on e.state = source.id
                    join transition t
                         on t.id = e.transition
                    join state_node target
                         on target.id = t.target
              where source.machine = %s
              $$,
              _machine_id
           ),
           $$ values ('c.root.a', 'c.root.b') $$
        );
end $f$;

create function test_target_id() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test4', 'test', '{
    "key": "root",
    "initial": "a",
    "states": {
      "a": {
        "always": ["#x"]
      },
      "b": {
        "id": "x"
      }
    }
  }');
begin
  return query
  select bag_eq(
           format($$
             select text(source.path) as source_path,
                    text(target.path) as target_path
               from state_node source
                    join eventless e
                         on e.state = source.id
                    join transition t
                         on t.id = e.transition
                    join state_node target
                         on target.id = t.target
              where source.machine = %s
             $$,
             _machine_id
           ),
           $$ values ('c.root.a', 'c.root.b') $$
         );
end $f$;

create function test_after() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test5', 'test', '{
    "key": "root",
    "initial": "a",
    "states": {
      "a": {
        "after": {
          "1000": "b"
        }
      },
      "b": {}
    }
  }');
begin
  return query
  select bag_eq(
           format(
             $$
             select text(source.path) as source_path,
                    text(target.path) as target_path,
                    a.duration
               from state_node source
                    join after a
                         on a.state = source.id
                    join transition t
                         on t.id = a.transition
                    join state_node target
                         on target.id = t.target
              where source.machine = %s
             $$,
             _machine_id
           ),
           $$ values ('c.root.a', 'c.root.b', '1s'::interval) $$
  );
end $f$;

create function test_invoke_external_service_as_object()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test7', 'test', '{
    "key": "root",
    "initial": "a",
    "states": {
      "a": {
        "invoke": {
          "src": {
            "type": "no_params_test"
          }         
        }
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select "type", external_service
        from invocation
       where exists (select from state_node where machine = %s and id = invocation.state)
      $$,
      _machine_id
    ),

    $$ values ('external'::service_type, 'no_params_test'::external_service_type) $$
  );
end $f$;

create function test_invoke_external_service_as_string()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test8', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "invoke": {
          "src": "no_params_test"
        }
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select "type", external_service
        from invocation
       where exists (select from state_node where machine = %s and id = invocation.state)
      $$,
      _machine_id
    ),
    $$ values ('external'::service_type, 'no_params_test'::external_service_type) $$
  );
end $f$;

create function test_invoke_external_service_with_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test9', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "invoke": {
          "src": {
            "type": "with_params_test",
            "int_param": 42,
            "string_param": "foo",
            "json_param": { "some": { "nested": "object" } }
          }
        }
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select "type",
             external_service,
             int_param,
             string_param,
             json_param
        from invocation
             join with_params_test_service_invocation
                  using (id)
       where exists (select from state_node where machine = %s and id = invocation.state)
      $$,
      _machine_id
    ),
    $$
    values (
      'external'::service_type,
      'with_params_test'::external_service_type,
      42,
      'foo',
      '{ "some": { "nested": "object" } }'::jsonb
    )
    $$
  );
end $f$;


create function test_invoke_external_service_with_single_ondone()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test10', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "invoke": {
          "src": "no_params_test",
          "onDone": "b"
        }
      },
      "b": {}
    }
  }');
begin
  return query select set_eq(
    format(
      $$
      select target.path
        from invocation
             join invocation_ondone
                  on invocation = invocation.id
             join transition
                  on transition = transition.id
             join state_node target
                  on transition.target = target.id

       where exists (select from state_node where machine = %s and id = invocation.state)
      $$,
      _machine_id
    ),
    $$ values ('c.root.b'::ltree) $$
  );
end $f$;

create function test_invoke_external_service_with_single_onerror()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test11', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "invoke": {
          "src": "no_params_test",
          "onError": "b"
        }
      },
      "b": {}
    }
  }');
begin
  return query select set_eq(
    format(
      $$
      select target.path
        from invocation
             join invocation_onerror
                  on invocation = invocation.id
             join transition
                  on transition.id = invocation_onerror.transition
             join state_node target
                  on target.id = transition.target

       where exists (select from state_node where machine = %s and id = invocation.state)
      $$,
      _machine_id
    ),
    $$ values ('c.root.b'::ltree) $$
  );
end $f$;

create function test_invoke_multiple_external_services_without_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test12', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "invoke": [
          { "src": "no_params_test" },
          { "src": "no_params_test" },
          { "src": "no_params_test" }
        ]
      }
    }
  }');
begin
  return query select bag_eq(
    format(
      $$
      select external_service
        from invocation
       where exists (select from state_node where machine = %s and id = invocation.state)        
      $$,
      _machine_id
    ),
    array['no_params_test', 'no_params_test', 'no_params_test']::external_service_type[]
  );
end $f$;

create function test_invoke_external_service_with_params_missing()
returns setof text language plpgsql as $f$
begin
  return query select throws_ok(
    $$
    select create_machine('test13', 'test', '{
      "initial": "a",
      "states": {
        "a": {
          "invoke": {
            "src": "with_params_test"
          }
        }
      }
    }');
    $$,
    'null value in column "string_param" of relation "with_params_test_service_invocation" violates not-null constraint'
  );
end $f$;

create function test_invoke_external_service_with_invalid_extra_param()
returns setof text language plpgsql as $f$
begin
  return query select throws_ok(
    $$
    select create_machine('test14', 'test', '{
      "initial": "a",
      "states": {
        "a": {
          "invoke": {
            "src": {
              "type": "no_params_test",
              "invalid_extra_param": "I am invalid"
            }
          }
        }
      }
    }')
    $$,
    'External service no_params_test does not accept any parameters, got {"invalid_extra_param": "I am invalid"}'
  );
end $f$;

create function test_compound_state_ondone()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test19', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "initial": "a_1",
        "a_1": {
          "type": "final"
        },
        "onDone": "b"
      },
      "b": {}
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select a.path as a_path,
             b.path as b_path
        from state_node a
             join state_ondone
                  on state_ondone.state = a.id
             join transition
                  on transition.id = state_ondone.transition
             join state_node b
                  on b.id = transition.target
       where a.machine = %s
             and b.machine = %1$s
      $$,
      _machine_id
    ),
    $$ values ('c.root.a'::ltree, 'c.root.b'::ltree) $$
  );
end $f$;

create function test_onentry_action_without_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test22', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "entry": "no_params_test"
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select action."type"
        from onentry_action
             join action
                  on action.id = onentry_action.action
       where exists (select from state_node where machine = %s and id = onentry_action.state)
      $$,
      _machine_id
    ),
    $$ values ('no_params_test'::action_type) $$
  );
end $f$;


create function test_onexit_action_without_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test24', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "exit": "no_params_test"
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select action."type"
        from onexit_action
             join action
                  on action.id = onexit_action.action
       where exists (select from state_node where machine = %s and id = onexit_action.state)
      $$,
      _machine_id
    ),
    $$ values ('no_params_test'::action_type) $$
  );
end $f$;


create function test_onentry_action_with_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test23', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "entry": {
          "type": "with_params_test",
          "int_param": 42,
          "string_param": "foo",
          "json_param": { "some": { "nested": "object" } }
        }
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select action.type, string_param, int_param, json_param
        from onentry_action
             join action
                  on action.id = onentry_action.action
             join with_params_test_action
                  on with_params_test_action.id = action.id
       where exists (select from state_node where machine = %s and id = onentry_action.state)
      $$,
      _machine_id
    ),
    $$
    values ('with_params_test'::action_type,
            'foo',
            42,
            '{ "some": { "nested": "object" } }'::jsonb)
    $$
  );
end $f$;


create function test_onexit_action_with_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test25', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "exit": {
          "type": "with_params_test",
          "int_param": 42,
          "string_param": "foo",
          "json_param": { "some": { "nested": "object" } }
        }
      }
    }
  }');
begin
  return query
  select set_eq(
    format(
      $$
      select action.type, string_param, int_param, json_param
        from onexit_action
             join action
                  on action.id = onexit_action.action
             join with_params_test_action
                  on with_params_test_action.id = action.id
       where exists (select from state_node where machine = %s and id = onexit_action.state)
      $$,
      _machine_id
    ),
    $$
    values ('with_params_test'::action_type,
            'foo',
            42,
            '{ "some": { "nested": "object" } }'::jsonb)
    $$
  );
end $f$;

create function test_multiple_state_tags()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test26', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "tags": ["foo", "bar"]
      }
    }
  }');
begin
  return query
  select is(
    (select tags from state_node where path = 'c.root.a' and machine = _machine_id),
    array['foo', 'bar']
  );
end $f$;

select do_tap('^test_');

select * from finish();
rollback;

