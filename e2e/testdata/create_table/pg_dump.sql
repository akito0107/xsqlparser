CREATE TABLE public.version (
    version_id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    project_id uuid NOT NULL,
    name character varying(256) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);