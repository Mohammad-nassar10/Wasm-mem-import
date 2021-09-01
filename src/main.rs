use wasmer::{Cranelift, Instance, Module, Store, Universal, imports};
// use wasmer_wasi::WasiState;
use wasmer::MemoryType;
use wasmer::Memory;
use std::ptr;
// use crate::types::{WasmModule, Pointer};

// A function that takes a path to `.wasm` file and returns a Pointer of type WasmModule.
#[no_mangle]
pub fn wasmInstance(path1: &str, path2: &str) {
    let wasm_bytes_file = std::fs::read(path1).unwrap();
    let store = Store::new(&Universal::new(Cranelift::default()).engine());
    // Compiling the Wasm module.
    let module = Module::new(&store, wasm_bytes_file).unwrap();
    // let mut wasi_env = WasiState::new("allocator").finalize().unwrap();
    let import_object = imports! {
        "env" => {
            // "sum" => sum_func,
            // "memory" => host_mem,
        }
    };
    // let import_object = wasi_env.import_object(&module).unwrap();
    let instance = Instance::new(&module, &import_object).unwrap();
    // let instance_ptr = Pointer::new(instance).into();
    let alloc_mem = instance.exports.get_memory("memory").unwrap();
    println!("alloc_mem = {:?}", alloc_mem);
    let wasm_bytes_file2 = std::fs::read(path2).unwrap();
    let store2 = Store::new(&Universal::new(Cranelift::default()).engine());
    // Compiling the Wasm module.
    let module2 = Module::new(&store2, wasm_bytes_file2).unwrap();
    // let mut wasi_env2 = WasiState::new("transformer").finalize().unwrap();
    let host_mem = Memory::new(&Store::default(),MemoryType::new(18, None, false)).unwrap();
    let import_object2 = imports! {
        "env" => {
            // "sum" => sum_func,
            "memory" => alloc_mem.clone(),
            // "memory" => host_mem,
        }
    };
    let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
    let read_wasm = instance.exports.get_native_function::<i64,i32>("read_from_addr").unwrap();
    let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
    let mem_ptr = alloc_mem.data_ptr();
    let alloc_ptr = alloc.call(100).unwrap();
    let alloc_mem = instance.exports.get_memory("memory").unwrap();
    println!("alloc_mem = {:?}", alloc_mem);
    println!("gg");
    let instance2 = Instance::new(&module2, &import_object2).unwrap();
    println!("gg");
    // let transformation_mem = instance2.exports.get_memory("memory").unwrap();
    // println!("transformation_mem = {:?}", transformation_mem);

    let read_wasm2 = instance2.exports.get_native_function::<i64,i32>("read_from_addr").unwrap();
    
    let mem_addr = mem_ptr as i64 + alloc_ptr as i64;
    unsafe { ptr::write(mem_addr as *mut i32, 12) };
    println!("mem addr = {:},, offset = {:?}", mem_addr, alloc_ptr);
    unsafe { println!("read = {:?}", std::ptr::read(mem_addr as *mut i32)) };
    println!("read wasm = {:?}", read_wasm.call(alloc_ptr as i64).unwrap());
    println!("read wasm2 = {:?}", read_wasm2.call(alloc_ptr as i64).unwrap());
}

fn main() {
    wasmInstance("../wasm-modules/allocator/target/wasm32-unknown-unknown/release/alloc.wasm", "../wasm-modules/transformation/target/wasm32-unknown-unknown/release/transformation.wasm");
    println!("Hello, world!");
}
