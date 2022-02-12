alter type condition_type add value 'always_false';
alter type condition_type add value 'always_true';

create function always_true(jsonb) returns bool language sql as 'select true';
create function always_false(jsonb) returns bool language sql as 'select false';

insert into condition_info (id, eval, has_params)
values ('always_false', 'always_false', false),
       ('always_true', 'always_true', false);

alter type condition_type add value 'with_params';

create function cond_with_params(jsonb) returns bool language sql as $$
  select $1->'int_param' = '42'::jsonb
         and $1->>'string_param' = 'foo'
$$;

create table with_params_guard (
  id int references guard primary key,
  string_param text not null,
  int_param int not null,
  json_param jsonb not null
);

insert into condition_info (id, eval, has_params)
values  ('with_params', 'cond_with_params', true);


alter type event_type add value 'no_params';

alter type action_type add value 'log';

create table log_action (
  id int references action primary key,
  message text not null
);


set search_path = public, utils, ltree, from_json;

begin;

select * from no_plan();


create function test_nested_compound_state_node() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test2', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "initial": "b",
        "states": {
          "b":{
            "initial": "c",
            "states": {
              "c": {
                "initial": "d",
                "states": {
                  "d": {
                    "initial": "e",
                    "states": {
                      "e": {}
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }');
  _service_id int := create_service(_machine_id);
  _current_state_node state_node;
begin
  select s.*
    into _current_state_node
    from state_node s
         join service
              on service.state = s.id

   where service.id = _service_id;

  return query select is('c.root.a.b.c.d.e', _current_state_node.path);
end $f$;

create function test_entry_action_is_queued() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test3', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "entry": { "type": "no_params_test" }
      }
    }
  }');
  _service_id int := create_service(_machine_id);
begin
  return query select set_eq(
    format(
      $$
      select "type"
        from action_job
             join action
                  on action.id = action
       where service = %s
      $$,
      _service_id
    ),
    $$ values ('no_params_test'::action_type) $$,
    'Entry action should get queued'
  );
end $f$;



create function test_simple_eventless_transition() returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test', 'test', '{
    "key": "machine",
    "initial": "a",
    "states": {
      "a": {
        "always": ["b"]
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
  _current_path ltree;
begin
  select path
    into _current_path
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current_path, 'c.machine.b');
end $f$;

create function test_multiple_eventless_transitions()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test5', 'test', '{
    "key": "machine",
    "initial": "a",
    "entry": {"type": "log", "message": "entry_root"},
    "states": {
      "a": {
        "entry": {"type": "log", "message": "entry_a"},
        "always": ["b"]
      },
      "b": {
        "entry": {"type": "log", "message": "entry_b"},
        "always": ["c"]
      },
      "c": {
        "entry": {"type": "log", "message": "entry_c"},
        "always": ["d"]
      },
      "d": {
        "entry": {"type": "log", "message": "entry_d"},
        "always": ["e"]
      },
      "e": {
        "entry": {"type": "log", "message": "entry_e"}
      }
    }
  }');
  _service_id int := create_service(_machine_id);
  _current_state_node state_node;
begin
  select s.*
    into _current_state_node
    from state_node s
         join service
              on service.state = s.id
   where service.id = _service_id;

  return query select is(_current_state_node.path, 'c.machine.e');

  return query select bag_eq(
    format(
      $$
        select message
          from action_job
               join log_action
                    on log_action.id = action_job.action
         where service = %s
      order by action_job.created_at
      $$,
      _service_id
    ),
    $$ values ('entry_root'),
              ('entry_a'),
              ('entry_b'),
              ('entry_c'),
              ('entry_d'),
              ('entry_e')
    $$
  );
end $f$;




create function test_eventless_transition_actions()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test6', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          {
            "target": "b",
            "actions": { 
              "type": "with_params_test",
              "int_param": 42,
              "string_param": "foo",
              "json_param": { "some": { "nested": "object" } }
            }
          }
        ]
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
begin
  return query
  select bag_eq(
    format(
      $$
      select "type"
        from action_job
             join action
                  on action.id = action_job.action
       where service = %s
      $$,
      _service_id
    ),
    $$ values ('with_params_test'::action_type) $$,
    'Eventless transition action should have been queued.'
  );
end $f$;

create function test_exit_action()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test7', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": ["b"],
        "exit": { 
          "type": "with_params_test", 
          "int_param": 999,
          "string_param": "hi there",
          "json_param": [1, 2, 3]
        }
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
begin
  return query select set_eq(
    format(
      $$
      select "type"
        from action
       where exists (select from action_job where service = %s and action.id = action)
      $$,
      _service_id
    ),
    $$ values ('with_params_test'::action_type) $$,
    'The exit action should have been queued.'
  );
end $f$;

create function test_entry_action_after_eventless_transition()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test8', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": ["b"]
      },
      "b": {
        "entry": { 
          "type": "with_params_test", 
          "int_param": 42,
          "string_param": "foo",
          "json_param": { "some": { "nested": "object" } }
        }
      }
    }
  }');
  _service_id int := create_service(_machine_id);
begin
  return query select set_eq(
    format(
      $$
      select "type"
        from action
       where exists (select from action_job where service = %s and action.id = action)
      $$,
      _service_id
    ),
    $$ values ('with_params_test'::action_type) $$,
    'The entry action should have been queued.'
  );
end $f$;

create function test_eventless_transition_with_unment_condition()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test9', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          { "target": "b", "cond": "always_false" }
        ]
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
  _current state_node;
begin
  select *
    into _current
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);
   
  return query select is(_current.path, 'c.root.a');
end $f$;

create function test_eventless_transition_with_matching_condition()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test10', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          {
            "target": "b",
            "cond": "always_true"
          }
        ]
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
  _current state_node;
begin
  select *
    into _current
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current.path, 'c.root.b');
end $f$;

create function test_matching_condition_with_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test19', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          {
            "target": "b",
            "cond": {
              "type": "with_params",
              "int_param": 42,
              "string_param": "foo",
              "json_param": { "some": { "nested": "object" } }
            }
          }
        ]
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
  _current state_node;
begin
  select *
    into _current
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current.path, 'c.root.b');
end $f$;

create function test_multiple_eventless_transitions_with_conditions()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test20', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "always": [
          { "target": "x", "cond": "always_false" },
          { "target": "y", "cond": "always_false" },
          { "target": "z", "cond": "always_true" }
        ]
      },
      "x": {},
      "y": {},
      "z": {}
    }
  }');
  _service_id int := create_service(_machine_id);
  _current state_node;
begin
  select *
    into _current
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current.path, 'c.root.z');
end $f$;


create function test_event_without_params_transition()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test21', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "on": {
          "no_params": "b"
        }
      },
      "b": {}
    }
  }');
  _service_id int := create_service(_machine_id);
  _current state_node;
  _event_id int;
begin
  insert into event ("type") values ('no_params') returning id into _event_id;

  perform send_event(_event_id, _service_id);

  select *
    into _current
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current.path, 'c.root.b');
end $f$;

create function test_entry_action_after_eventful_transition()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test22', 'test', '{
    "initial": "x",
    "states": {
      "x": {
        "on": {
          "no_params": "y"
        }
      },
      "y": {
        "entry": "no_params_test"
      }
    }
  }');
  _service_id int := create_service(_machine_id);
  _event_id int;
begin
  insert into event ("type") values ('no_params') returning id into _event_id;

  perform send_event(_event_id, _service_id);

  return query select is(
    (select path from state_node where exists (select from service where id = _service_id and state = state_node.id)),
    'c.root.y'
  );

  return query select set_eq(
    format(
      $$
      select "type"
        from action
       where exists (select from action_job where service = %s and action.id = action_job.action)
      $$,
      _service_id
    ),
    $$ values ('no_params_test'::action_type) $$,
    'Entry action should have been queued.'
  );

end $f$;



create function test_deeply_nested_compound_state()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test23', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "initial": "a_1", 
        "states": {
          "a_1": {
            "initial": "a_1_1",
            "states": {
              "a_1_1": {
                "initial": "a_1_1_1",
                "states": {
                  "a_1_1_1": {}
                }
              }
            }
          }
        }
      },
      "b": {
        "id": "b"
      }
    },
    "on": {
      "no_params": "#b"
    }
  }');
  _service_id int := create_service(_machine_id);
  _event_id int;
  _path ltree;
begin
  insert into event ("type") values ('no_params') returning id into _event_id;

  perform send_event(_event_id, _service_id);

  select path
    into _path
    from state_node
   where exists (select from service where state = state_node.id and id = _service_id);

  return query select is(_path, 'c.root.b');
end $f$;

create function test_choose_the_innermost_eventless_transition_first()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test30', 'test', '{
    "initial": "a",
    "states": {
        "a": {
            "initial": "a_1",
            "always": ["#from_a"],
            "states": {
                "a_1": {
                    "initial": "a_1_1",
                    "states": {
                        "a_1_1": {
                            "always": ["#from_a_1_1"]
                        }
                    }
                }
            }
        },
        "from_a_1_1": {
            "id": "from_a_1_1"
        },
        "from_a": {
            "id": "from_a"
        }
    }
  }');
  _service_id int := create_service(_machine_id);
  _current_path ltree;
begin
  select path
    into _current_path
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current_path, 'c.root.from_a_1_1');
end $f$;


create function test_follows_eventless_transitions_further_up_the_state_hierarchy()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test31', 'test', '{
    "initial": "a",
    "states": {
        "a": {
            "initial": "a_1",
            "always": ["#x"],
            "states": {
                "a_1": {
                    "initial": "a_1_1",
                    "states": {
                        "a_1_1": {}
                    }
                }
            }
        },
        "x": {
            "id": "x"
        }
    }
  }');
  _service_id int := create_service(_machine_id);
  _current_path ltree;
begin
  select path
    into _current_path
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_current_path, 'c.root.x');
end $f$;




create function test_choose_the_innermost_event_mapping_first()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test24', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "initial": "a_1",
        "states": {
          "a_1": {
            "initial": "a_1_1",
            "states": {
              "a_1_1": {
                "on": {
                  "no_params": "#from_a_1_1"
                }
              }
            }
          }
        }
      },
      "from_a_1_1": {
        "id": "from_a_1_1"
      },
      "from_root": {
        "id": "from_root"
      }
    },
    "on": {
      "no_params": "#from_root"
    }
  }');
  _service_id int := create_service(_machine_id);
  _event_id int;
  _path ltree;
begin
  insert into event ("type") values ('no_params') returning id into _event_id;

  perform send_event(_event_id, _service_id);

  select path
    into _path
    from state_node
   where exists (select from service where id = _service_id and state = state_node.id);

  return query select is(_path, 'c.root.from_a_1_1');
end $f$;

create function _test_invoke_without_params()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test25', 'test', '{
    "initial": "a",
    "states": {
      "a": {
        "invoke": {
          "src": "no_params_test",
          "onDone": "done"
        }
      },
      "done": {}
    }
  }');
  _service_id int := create_service(_machine_id);
begin
  return query select set_eq(
    format(
      $$
      select "type",
             external_service
        from service
       where parent = %s
      $$,
      _service_id
    ),
    $$ values ('external'::service_type, 'no_params_test'::external_service_type) $$
  );

end $f$;



create function test_complicated_entry_actions()
returns setof text language plpgsql as $f$
declare
  _machine_id int := create_machine('test26', 'test', '{
    "initial": "a",
    "entry": { "type": "log", "message": "entry_root" },
    "states": {
      "a": {
        "initial": "a_1",
        "entry": { "type": "log", "message": "entry_a" },
        "states": {
          "a_1": {
            "initial": "a_1_1",
            "entry": { "type": "log", "message": "entry_a_1" },
            "states": {
              "a_1_1": {
                "entry": { "type": "log", "message": "entry_a_1_1" },
                "always": ["#a_2_1_1"]
              }
            }
          },
          "a_2": {
            "initial": "a_2_1",
            "entry": { "type": "log", "message": "entry_a_2" },
            "states": {
              "a_2_1": {
                "initial": "a_2_1_1",
                "entry": { "type": "log", "message": "entry_a_2_1" },
                "states": {
                  "a_2_1_1": {
                    "id": "a_2_1_1",
                    "entry": { "type": "log", "message": "entry_a_2_1_1" }
                  }
                }
              }
            }
          }
        }
      }
    }
  }');
  _service_id int := create_service(_machine_id);
  _transition_id int;
begin
  select id
    into _transition_id
    from transition
   where exists (select from state_node where id = transition.target and machine = _machine_id);

  return query select bag_eq(
    format(
      $$
      select message
        from action
             join action_job
                  on action.id = action_job.action
             join log_action
                  on log_action.id = action.id
       where service = %s
      $$,
      _service_id
    ),
    $$
    values ('entry_root'),
           ('entry_a'),
           ('entry_a_1'),
           ('entry_a_1_1'),
           ('entry_a_2'),
           ('entry_a_2_1'),
           ('entry_a_2_1_1')
    $$
  );
end $f$;


select do_tap('^test_');

select * from finish();
rollback;

