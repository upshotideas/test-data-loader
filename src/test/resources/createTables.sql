create table client (
    id bigserial primary key,
    name varchar(512) not null,
    status varchar(255) not null,
    r_cre_id varchar(512) null,
    r_cre_time timestamp null,
    r_mod_id varchar(512) null,
    r_mod_time timestamp null
);

create table second_table (
	id bigserial primary key,
	client_id bigint not null,
	display_name varchar(512) null,
	some_type varchar(255) not null,
	status varchar(255) not null,
	r_cre_id varchar(512) null,
	r_cre_time timestamp null,
	r_mod_id varchar(512) null,
	r_mod_time timestamp null
);

create table third_table (
	id bigserial primary key
);