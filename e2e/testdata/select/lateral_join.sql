SELECT n.id, n.name, t.max, t.min FROM node n,
    LATERAL (
        SELECT max(usage) as max, min(usage) as min
        FROM node_mon m
        WHERE m.id = n.id
    ) t;
