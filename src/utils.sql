
create function insert_from_json(tbl text, dat jsonb) returns void language plpgsql as $$
declare
  _columns text;
  _values text;
begin
  select string_agg(format('%I', key), ', ' order by key),
         string_agg(
           case
             when jsonb_typeof(value) = 'string' then format('%L', value #>> '{}')
             else format('%L', value)
           end,
           ', ' order by key
         )

   into _columns,
        _values

   from jsonb_each(dat);

  execute format('INSERT INTO %I (%s) VALUES (%s)', tbl, _columns, _values);
end $$;






create function proper_ancestor(
  ltree,
  ltree
)
returns bool language sql immutable as $$
  select $1 @> $2 and $1 <> $2
$$;




create function longest_common_prefix(ltree, ltree)
returns ltree language sql immutable as $$
  select case
           when $1 @> $2 then $1
           when $2 @> $1 then $2
           else lca($1, $2)
         end
$$;
comment on function longest_common_prefix is
$$
@test longest_common_prefix('a.b.c', 'a.b.c') = 'a.b.c'
@test longest_common_prefix('a.b.c.c', 'a') = 'a'
$$;
