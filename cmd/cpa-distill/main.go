package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);
typedef struct {
 uint32_t abi_version;
 void* host_ctx;
 cliproxy_host_call_fn call;
 cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct {
 uint32_t abi_version;
 cliproxy_plugin_call_fn call;
 cliproxy_plugin_free_fn free_buffer;
 cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;
extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"unsafe"

	"github.com/mars160/cpa-distill/internal/plugin"
)

var instance = plugin.New()

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, api *C.cliproxy_plugin_api) C.int {
	if host == nil || api == nil || host.abi_version != 1 {
		return 1
	}
	api.abi_version = 1
	api.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	api.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	api.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, length C.size_t, response *C.cliproxy_buffer) (status C.int) {
	if response == nil {
		return 1
	}
	response.ptr, response.len = nil, 0
	transparent := false
	defer func() {
		if recover() != nil {
			if response.ptr != nil {
				C.free(response.ptr)
			}
			response.ptr, response.len = nil, 0
			if transparent {
				writeResponse(response, []byte(`{"ok":true,"result":{}}`))
				status = 0
			} else {
				writeResponse(response, []byte(`{"ok":false,"error":{"code":"panic","message":"capture plugin internal failure"}}`))
				status = 1
			}
		}
	}()
	// C.GoBytes accepts a signed C.int. Never silently truncate a host buffer.
	if method == nil || uint64(length) > 2147483647 || (length > 0 && request == nil) {
		writeResponse(response, []byte(`{"ok":false,"error":{"code":"invalid_request","message":"invalid ABI arguments"}}`))
		return 1
	}
	var raw []byte
	if length > 0 {
		raw = C.GoBytes(unsafe.Pointer(request), C.int(length))
	}
	name := C.GoString(method)
	switch name {
	case "request.intercept_before", "request.intercept_after", "response.intercept_after", "response.intercept_stream_chunk", "request.complete", "websocket.response_event", "usage.handle":
		transparent = true
	}
	writeResponse(response, instance.Handle(name, raw))
	// Semantic failures use the JSON envelope, matching the host RPC contract.
	return 0
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if len(raw) == 0 {
		return
	}
	response.ptr = C.CBytes(raw)
	response.len = C.size_t(len(raw))
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, length C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() { _ = instance.Close() }
