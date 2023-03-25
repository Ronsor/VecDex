CC ?= gcc
CFLAGS ?= -fPIC
LDFLAGS ?=

OBJ = vecdex.o
DLL = libvecdex.so

.c.o:
	$(CC) -c -o $@ $< $(CFLAGS)

$(DLL): $(OBJ)
	$(CC) -shared -o $@ $(OBJ) $(LDFLAGS)

clean:
	rm -f *.so *.a *.o
