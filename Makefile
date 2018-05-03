BIN := bin

GLIB_LIB_FLAGS := `pkg-config --libs glib-2.0`
GLIB_INC_FLAGS := `pkg-config --cflags glib-2.0`

CFLAGS := -g -O3 -std=gnu99 -Wall -I/usr/include -I. $(GLIB_INC_FLAGS)
SOFLAGS := $(GLIB_LIB_FLAGS) -lrt
LDFLAGS := -L$(BIN) -lrt $(GLIB_LIB_FLAGS)
LIBDEPENDS := $(BIN)

all: $(BIN)/logsender

$(BIN)/logsender: $(BIN) logsender.c  
	gcc logsender.c $(CFLAGS) -o $(BIN)/logsender $(LDFLAGS)

bin:
	mkdir -p bin

clean:
	rm -rf $(BIN)
