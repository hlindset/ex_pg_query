MIX = mix
CFLAGS = -O3 -Wall
ERLANG_PATH = $(shell erl -eval 'io:format("~s", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version), "/include"])])' -s init stop -noshell)
CFLAGS += -I$(ERLANG_PATH)
CFLAGS += -I./libpg_query/vendor
LIBPG_QUERY_PATH = libpg_query

CFLAGS += -I$(LIBPG_QUERY_PATH) -fPIC

LDFLAGS = -lpthread -shared
ifeq ($(shell uname -s),Darwin)
    LDFLAGS += -undefined dynamic_lookup
endif

.PHONY: all ex_pg_query clean update-libpg_query

all: priv/ex_pg_query.so

priv:
	mkdir -p priv

$(LIBPG_QUERY_PATH)/libpg_query.a:
	$(MAKE) -B -C $(LIBPG_QUERY_PATH) libpg_query.a

priv/ex_pg_query.so: priv $(LIBPG_QUERY_PATH)/libpg_query.a src/ex_pg_query.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ src/ex_pg_query.c $(LIBPG_QUERY_PATH)/libpg_query.a

clean:
	$(MIX) clean
	$(MAKE) -C $(LIBPG_QUERY_PATH) clean
	$(RM) priv/ex_pg_query.so
