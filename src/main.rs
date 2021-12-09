// use arrow::array::ArrayRef;
// use wasmer::{ChainableNamedResolver, ImportObject};
// use wasmer::{Cranelift, Function, Instance, Module, Store, Universal, imports};
// use wasmer_wasi::WasiState;
// use wasmer::MemoryType;
// use wasmer::Memory;
// use std::ptr;
// use arrow::{array::StructArray, record_batch::RecordBatch, util::pretty::print_batches};
// // use crate::types::{WasmModule, Pointer};

// // A function that takes a path to `.wasm` file and returns a Pointer of type WasmModule.
// #[no_mangle]
// pub fn wasmInstance(path1: &str, path2: &str) {
//     print_batches(results)
//     let x: ArrayRef;
//     let y = x.as_any().downcast_ref::<StructArray>().unwrap().columns().as_slice();
//     x.data().buffers().as_chunks()
//     let wasm_bytes_file = std::fs::read(path1).unwrap();
//     let store = Store::new(&Universal::new(Cranelift::default()).engine());
//     // Compiling the Wasm module.
//     let module = Module::new(&store, wasm_bytes_file).unwrap();
//     let mut wasi_env = WasiState::new("allocator").finalize().unwrap();

//     fn multiply(a: i32) {
//         println!("Calling `multiply_native`...");
//         let result = a * 3;

//         println!("Result of `multiply_native`: {:?}", result);

//         // result
//     }
//     let multiply_native = Function::new_native(&store, multiply);



//     let import_object = imports! {
//         "env" => {
//             "func" => multiply_native,
//             // "sum" => sum_func,
//             // "memory" => host_mem,
//         }
//     };
//     let import_object = wasi_env.import_object(&module).unwrap();
//     println!("import object wasi = {:?}", import_object);
//     let instance = Instance::new(&module, &import_object).unwrap();
//     // let instance_ptr = Pointer::new(instance).into();
//     let alloc_mem = instance.exports.get_memory("memory").unwrap();
//     println!("alloc_mem = {:?}", alloc_mem);
    
//     let alloc = instance.exports.get_native_function::<i64,i32>("alloc").unwrap();
//     // let read_wasm = instance.exports.get_native_function::<i64,i32>("read_from_addr").unwrap();
//     // let dealloc = instance.exports.get_native_function::<(i64,i64),()>("dealloc").unwrap();
//     // let mem_ptr = alloc_mem.data_ptr();
//     let alloc_ptr = alloc.call(100).unwrap();
//     // let alloc_mem = instance.exports.get_memory("memory").unwrap();
//     // println!("alloc_mem = {:?}", alloc_mem);
//     // println!("gg");
//     // // let transformation_mem = instance2.exports.get_memory("memory").unwrap();
//     // // println!("transformation_mem = {:?}", transformation_mem);

//     // let read_wasm2 = instance2.exports.get_native_function::<i64,i32>("read_from_addr").unwrap();
    
//     // let mem_addr = mem_ptr as i64 + alloc_ptr as i64;
//     // unsafe { ptr::write(mem_addr as *mut i32, 12) };
//     // println!("mem addr = {:},, offset = {:?}", mem_addr, alloc_ptr);
//     // unsafe { println!("read = {:?}", std::ptr::read(mem_addr as *mut i32)) };
//     // println!("read wasm = {:?}", read_wasm.call(alloc_ptr as i64).unwrap());
//     // println!("read wasm2 = {:?}", read_wasm2.call(alloc_ptr as i64).unwrap());
// }

// fn main() {
//     wasmInstance("wasmModule/target/wasm32-wasi/release/wasmModule.wasm", "wasm-modules/transformation.wasm");
//     println!("Hello, world!");
// }
















use arrow::{record_batch::RecordBatch, array::{Int64Array, StructArray, Array, make_array_from_raw}, datatypes::{Schema, Field, DataType}, ffi::{FFI_ArrowArray, FFI_ArrowSchema}};
// use wasmer::{Cranelift, Instance, Module, Store, Universal, imports};
// // use wasmer_wasi::WasiState;
// use wasmer::MemoryType;
// use wasmer::Memory;
use wasmtime::*;
use wasmtime_wasi::{sync::WasiCtxBuilder, WasiCtx};
use std::{ptr, sync::Arc};
// use crate::types::{WasmModule, Pointer};


fn create_record_batch() -> RecordBatch {
    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let id_array2 = Int64Array::from(vec![1, 2, 3, 4, 5]);
    // let schema = Schema::new(vec![
    // Field::new("id", DataType::Int64, false)
    // ]);

    let projected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(projected_schema),
        vec![
            Arc::new(id_array),
            Arc::new(id_array2),
        ],
    );

    // let batch = RecordBatch::try_new(
    // Arc::new(schema),
    // vec![Arc::new(id_array)]
    // ).unwrap();
    batch.unwrap()
}


// A function that takes a path to `.wasm` file and returns a Pointer of type WasmModule.
#[no_mangle]
pub fn wasmInstance(path1: &str, path2: &str) -> (TypedFunc<i64, i32>, Store<WasiCtx>) {

    // let batch = create_record_batch();
    // println!("record batch = {:?}", batch);
    // let struct_array: StructArray = batch.into();
    // let (in_array, in_schema) = struct_array.to_raw().unwrap();
    // unsafe { println!("ffi_schema = {:?}", *((in_schema as u64) as *mut FFI_ArrowSchema)) };


    // An engine stores and configures global compilation settings like
    // optimization level, enabled wasm features, etc.
    let engine = Engine::default();
    let mut linker = Linker::new(&engine);
    wasmtime_wasi::add_to_linker(&mut linker, |s| s).unwrap();
    // We start off by creating a `Module` which represents a compiled form
    // of our input wasm module. In this case it'll be JIT-compiled after
    // we parse the text format.
    let module1 = Module::from_file(&engine, path1).unwrap();
    let module2 = Module::from_file(&engine, path2).unwrap();

    let wasi = WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_args().unwrap()
        .build();

    let mut store = Store::new(&engine, wasi);
    let instance2 = linker.instantiate(&mut store, &module2).unwrap();
    linker.instance(&mut store, "env", instance2).unwrap();
    // A `Store` is what will own instances, functions, globals, etc. All wasm
    // items are stored within a `Store`, and it's what we'll always be using to
    // interact with the wasm world. Custom data can be stored in stores but for
    // now we just use `()`.
    // let mut store = Store::new(&engine, ());
    let callback_func = Func::wrap(&mut store, |a: i32, b: i64| -> (i64, i32) {
        (b + 1, a + 1)
    });
    // With a compiled `Module` we can then instantiate it, creating
    // an `Instance` which we can actually poke at functions on.
    let instance1 = linker.instantiate(&mut store, &module1).unwrap();
    let instance0 = linker.instantiate(&mut store, &module1).unwrap();
    // let instance = Instance::new(&mut store, &module, &[]).unwrap();

    // let memory1 = instance1
    //     .get_memory(&mut store, "memory").unwrap();
    let memory2 = instance2
        .get_memory(&mut store, "memory").unwrap();
    println!("memory = {:?}", memory2.size(&store));
    let mem_ptr = memory2.data_ptr(&store);

    let create_batch = instance2.get_func(&mut store, "create_record_batch")
    .expect("`create` was not an exported function").typed::<(), i64, _>(&store).unwrap();
    // let create_batch = create_batch.typed::<(), i64, _>(&store).unwrap();
    let batch = create_batch.call(&mut store, ()).unwrap();
    println!("res of create call = {:?}", batch);


    let out_batch = batch;
    let transform_func = instance2.get_func(&mut store, "transform_ipc")
    .expect("`transform` was not an exported function");
    let transform_func = transform_func.typed::<i64, i64, _>(&store).unwrap();
    let out_batch = transform_func.call(&mut store, batch).unwrap();
    println!("res of transform call = {:?}", out_batch);

    // let batch = out_batch;

    // let second_elem = instance1.get_func(&mut store, "get_second_of_tuple")
    // .expect("`second` was not an exported function");
    // let second_elem = second_elem.typed::<i64, i64, _>(&store).unwrap();
    // let ffi_schema = second_elem.call(&mut store, batch as i64).unwrap();
    // // unsafe { println!("ffi_schema = {:?}", *((ffi_schema + mem_ptr as i64) as *mut FFI_ArrowSchema)) };

    // let first_elem = instance1.get_func(&mut store, "get_first_of_tuple")
    // .expect("`first` was not an exported function");
    // let first_elem = first_elem.typed::<i64, i64, _>(&store).unwrap();
    // let ffi_array = first_elem.call(&mut store, batch as i64).unwrap();
    // // unsafe { println!("ffi_array = {:?}", *((ffi_array + mem_ptr as i64) as *mut FFI_ArrowArray)) };

    // let create_tuple = instance1.get_func(&mut store, "create_tuple_ptr")
    // .expect("`first` was not an exported function");
    // let create_tuple = create_tuple.typed::<(i64, i64), i64, _>(&store).unwrap();
    // let batch = create_tuple.call(&mut store, (ffi_array, ffi_schema)).unwrap();


    
    // let transform_func2 = instance2.get_func(&mut store, "transform_ffi2")
    // .expect("`transform` was not an exported function");
    // let transform_func2 = transform_func2.typed::<(i64, i64), i64, _>(&store).unwrap();
    // let out_batch2 = transform_func2.call(&mut store, (ffi_array, ffi_schema)).unwrap();
    // println!("res of transform call = {:?}", out_batch2);
    

    let transform_func1 = instance1.get_func(&mut store, "transform_ipc")
    .expect("`transform` was not an exported function");
    let transform_func1 = transform_func1.typed::<i64, i64, _>(&store).unwrap();
    let out_batch = transform_func1.call(&mut store, out_batch).unwrap();
    println!("res of transform call = {:?}", out_batch);

    let transform_func0 = instance0.get_func(&mut store, "transform_ipc")
    .expect("`transform` was not an exported function");
    let transform_func0 = transform_func0.typed::<i64, i64, _>(&store).unwrap();
    let out_batch = transform_func0.call(&mut store, out_batch).unwrap();
    println!("res of transform call = {:?}", out_batch);
    // let out_array = first_elem.call(&mut store, out_batch as i64).unwrap();
    // let out_schema = second_elem.call(&mut store, out_batch as i64).unwrap();
    // println!("out_array = {:?}, out_Schema = {:?}", out_array, out_schema);
    // let memory2 = instance2
    //     .get_memory(&mut store, "memory").unwrap();
    // println!("memory = {:?}", memory2.size(&store));
    // let mem_ptr = memory2.data_ptr(&store);
    // unsafe { let read = ptr::read((out_array + mem_ptr as i64) as *mut i32);
    //             println!("read = {:?}", read); }
    
    // unsafe { println!("schema = {:?}", *((out_schema + mem_ptr as i64) as *mut FFI_ArrowSchema)); }            

    // let array = unsafe { make_array_from_raw((out_array + mem_ptr as i64) as *mut FFI_ArrowArray, (out_schema + mem_ptr as i64) as *mut FFI_ArrowSchema).unwrap() };
    // let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    // let output = RecordBatch::from(as_structarray);
    // println!("output batch = {:?}", output);


    let alloc_func = instance1.get_func(&mut store, "alloc")
    .expect("`answer` was not an exported function");
    let alloc = alloc_func.typed::<i64, i32, _>(&store).unwrap();
    let res = alloc.call(&mut store, 50);
    let alloc_ptr = res.unwrap();
    println!("res = {:?}", alloc_ptr);
    let alloc_func2 = instance2.get_func(&mut store, "alloc")
    .expect("`answer` was not an exported function");
    let alloc2 = alloc_func2.typed::<i64, i32, _>(&store).unwrap();
    let res2 = alloc2.call(&mut store, 50);
    println!("res = {:?}", res2.unwrap());

    let mem_ptr1 = memory2.data_ptr(&store);
    let mem_ptr2 = memory2.data_ptr(&store);
    println!("mem ptr 1 = {:?}, mem ptr 2 = {:?}", mem_ptr1, mem_ptr2);

    let mem_addr = mem_ptr1 as i64 + alloc_ptr as i64;
    // unsafe { ptr::write(mem_addr as *mut i32, 15) };
    let read_func2 = instance0.get_func(&mut store, "read_from_addr")
    .expect("`read` was not an exported function");
    let read_func2 = read_func2.typed::<i64, i32, _>(&store).unwrap();
    let read_res2 = read_func2.call(&mut store, alloc_ptr as i64);
    println!("read res = {:?}", read_res2.unwrap());
    // println!("mem addr = {:},, offset = {:?}", mem_addr, alloc_ptr);
    // unsafe { println!("read = {:?}", std::ptr::read(mem_addr as *mut i32)) };
    // println!("read wasm = {:?}", read_wasm.call(alloc_ptr as i64).unwrap());
    // println!("read wasm2 = {:?}", read_wasm2.call(alloc_ptr as i64).unwrap());

(alloc, store)


    // let wasm_bytes_file = std::fs::read(path1).unwrap();
    // let store = Store::new(&Universal::new(Cranelift::default()).engine());
    // // Compiling the Wasm module.
    // let module = Module::new(&store, wasm_bytes_file).unwrap();
    // // let mut wasi_env = WasiState::new("allocator").finalize().unwrap();
    // let import_object = imports! {
    //     "env" => {
    //     }
    // };
    // // let import_object = wasi_env.import_object(&module).unwrap();
    // let instance = Instance::new(&module, &import_object).unwrap();
    // // let instance_ptr = Pointer::new(instance).into();
    // let alloc_mem = instance.exports.get_memory("memory").unwrap();
    // println!("alloc_mem = {:?}", alloc_mem.data_ptr());
    // let wasm_bytes_file2 = std::fs::read(path2).unwrap();
    // let store2 = Store::new(&Universal::new(Cranelift::default()).engine());
    // // Compiling the Wasm module.
    // let module2 = Module::new(&store2, wasm_bytes_file2).unwrap();
    // // let mut wasi_env2 = WasiState::new("transformer").finalize().unwrap();
    // let host_mem = Memory::new(&Store::default(),MemoryType::new(18, None, false)).unwrap();
    // let import_object2 = imports! {
    //     "env" => {
    //         // "sum" => sum_func,
    //         "memory" => alloc_mem.clone(),
    //         // "memory" => host_mem,
    //     }
    // };
    // let alloc = instance.exports.get_native_function::<i64, i32>("alloc").unwrap();
    // let read_wasm = instance.exports.get_native_function::<i64, i32>("read_from_addr").unwrap();
    // let dealloc = instance.exports.get_native_function::<(i64, i64),()>("dealloc").unwrap();
    // let mem_ptr = alloc_mem.data_ptr();
    // // let alloc_mem = instance.exports.get_memory("memory").unwrap();
    // // println!("alloc_mem = {:?}", alloc_mem);
    // println!("gg");
    // let instance2 = Instance::new(&module2, &import_object2).unwrap();
    // let alloc_ptr = alloc.call(100000).unwrap();
    // println!("gg");
    // let alloc2 = instance2.exports.get_native_function::<i64, i32>("alloc").unwrap();
    // let alloc_ptr2 = alloc2.call(100000).unwrap();
    // // let alloc_mem2 = instance2.exports.get_memory("memory").unwrap();
    // // println!("alloc_mem2 = {:?}", alloc_mem2.data_ptr());
    // // let transformation_mem = instance2.exports.get_memory("memory").unwrap();
    // // println!("transformation_mem = {:?}", transformation_mem);

    // // let read_wasm2 = instance2.exports.get_native_function::<i64,i32>("read_from_addr").unwrap();
    
    // // let mem_addr = mem_ptr as i64 + alloc_ptr as i64;
    // // unsafe { ptr::write(mem_addr as *mut i32, 12) };
    // // println!("mem addr = {:},, offset = {:?}", mem_addr, alloc_ptr);
    // // unsafe { println!("read = {:?}", std::ptr::read(mem_addr as *mut i32)) };
    // // println!("read wasm = {:?}", read_wasm.call(alloc_ptr as i64).unwrap());
    // // println!("read wasm2 = {:?}", read_wasm2.call(alloc_ptr as i64).unwrap());
}

fn main() {
    // wasmInstance("/home/mohammadtn/Wasm-mem-import/wasmModule2/target/wasm32-unknown-unknown/release/wasmModule2.wasm", "/home/mohammadtn/Wasm-mem-import/wasmModule/target/wasm32-unknown-unknown/release/wasmModule.wasm");
    let (func, mut store) = wasmInstance("/home/mohammadtn/Wasm-mem-import/wasmModule2/target/wasm32-unknown-unknown/release/wasmModule2.wasm", "/home/mohammadtn/Wasm-mem-import/wasmModule/target/wasm32-wasi/release/wasmModule.wasm");
    let res = func.call(&mut store, 50000).unwrap();
    println!("end, res = {:?}", res);
}
