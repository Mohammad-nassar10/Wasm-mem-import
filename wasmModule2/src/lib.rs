
use std::mem;
use std::sync::Arc;
use std::ops::Deref;
use std::io::Cursor;
use std::os::raw::c_void;
// use arrow::array::ArrayRef;
// use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use arrow::{ipc::{self, reader::StreamReader}};
use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use arrow::compute::kernels::filter::filter_record_batch;
use arrow::{array::{Int32Array, Int64Array, PrimitiveArray, StructArray, Array, ArrayRef, BooleanArray, make_array_from_raw}, compute::unary, datatypes::{DataType, Field, Int32Type, Int64Type, Schema}, record_batch::RecordBatch};
use arrow::compute::kernels::comparison::{eq_scalar, neq_scalar, gt_scalar, gt_eq_scalar, lt_scalar, lt_eq_scalar};



#[derive(Debug)]
pub struct Pointer<Kind> {
    value: Box<Kind>,
}

impl<Kind> Pointer<Kind> {
    pub fn new(value: Kind) -> Self {
        Pointer {
            value: Box::new(value),
        }
    }

    pub fn borrow<'a>(self) -> &'a mut Kind {
        Box::leak(self.value)
    }
}

impl<Kind> From<Pointer<Kind>> for i64 {
    fn from(pointer: Pointer<Kind>) -> Self {
        Box::into_raw(pointer.value) as _
    }
}

impl<Kind> From<i64> for Pointer<Kind> {
    fn from(pointer: i64) -> Self {
        Self {
            value: unsafe { Box::from_raw(pointer as *mut Kind) },
        }
    }
}

impl<Kind> Deref for Pointer<Kind> {
    type Target = Kind;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Tuple (pub i64, pub i64 );

#[no_mangle]
pub fn alloc(len: i64) -> *mut c_void {
    // unsafe{func(5);}
    // create a new mutable buffer with capacity `len`
    let mut buf = Vec::with_capacity(len as usize);
    // take a mutable pointer to the buffer
    let ptr = buf.as_mut_ptr();
    // take ownership of the memory block and
    // ensure that its destructor is not
    // called when the object goes out of scope
    // at the end of the function
    std::mem::forget(buf);
    // return the pointer so the runtime
    // can write data at this offset
    ptr as *mut c_void
}

#[no_mangle]
pub unsafe fn dealloc(ptr: i64, size: i64) {
    let data = Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize);
    std::mem::drop(data);
}
pub fn transform_record_batch(record_in: RecordBatch) -> RecordBatch {
    let filter_col = "a";
    let filter_val = 3;
    let filter_op = "<";
    
    let num_rows = record_in.num_rows();
    // Get the columns of the input record batch
    let columns: &[ArrayRef] = record_in.columns();

    // Generate a boolean vector, indicating whether the
    // row corresponds to an underage individual

    // Get the index of the column which we want to filter according to it
    let filter_index = record_in.schema().index_of(filter_col).unwrap();
    let filter_column = columns[filter_index].data();
    let filter_array = Int64Array::from(filter_column.clone());

    let bool_arr = match filter_op {
        "=" => eq_scalar::<Int64Type>(&filter_array, filter_val).unwrap(),
        "!=" => neq_scalar::<Int64Type>(&filter_array, filter_val).unwrap(),
        ">=" => gt_eq_scalar::<Int64Type>(&filter_array, filter_val).unwrap(),
        ">" => gt_scalar::<Int64Type>(&filter_array, filter_val).unwrap(),
        "<=" => lt_eq_scalar::<Int64Type>(&filter_array, filter_val).unwrap(),
        "<" => lt_scalar::<Int64Type>(&filter_array, filter_val).unwrap(),
        _ => BooleanArray::from(vec![true; num_rows]),
    };

    let transformed_record = filter_record_batch(&record_in, &bool_arr).unwrap();
    transformed_record
}

pub fn transform_record_batch1(record_in: RecordBatch) -> RecordBatch {
    let num_cols = record_in.num_columns();
    let num_rows = record_in.num_rows();
    // Build a zero array
    let struct_array = Int64Array::from(vec![0; num_rows]);
    let new_column = Arc::new(struct_array);
    // Get the columns except the last column
    let columns: &[ArrayRef] = record_in.columns();
    let first_columns = columns[0..num_cols-1].to_vec();
    // Create a new array with the same columns expect the last where it will be zero column
    let new_array = [first_columns, vec![new_column]].concat();
    // Create a transformed record batch with the same schema and the new array
    let transformed_record = RecordBatch::try_new(
        record_in.schema(),
        new_array
    ).unwrap();
    transformed_record
}

#[no_mangle]
pub fn create_tuple_ptr(elem1: i64, elem2: i64) -> i64 {
    let ret_tuple = Tuple(elem1, elem2);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
}

 //////////IPC related functions//////////

// #[no_mangle]
// pub fn read_transform_write_from_bytes(bytes_ptr: i64, bytes_len: i64) -> i64 {
//     // Read the byte array in the given address and length
//     let bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts(bytes_ptr as *mut _, bytes_len as usize, bytes_len as usize) };
//     let cursor = Cursor::new(bytes_array);
//     let reader = StreamReader::try_new(cursor).unwrap();
//     let mut ret_ptr = 0;
//     reader.for_each(|batch| {
//         let batch = batch.unwrap();
//         // Transform the record batch
//         let transformed = transform_record_batch(batch);

//         // Write the transformed record batch uing IPC
//         let schema = transformed.schema();
//         let vec = Vec::new();
//         let mut writer = crate::ipc::writer::StreamWriter::try_new(vec, &schema).unwrap();
//         writer.write(&transformed).unwrap();
//         writer.finish().unwrap();
//         let mut bytes_array = writer.into_inner().unwrap();
//         bytes_array.shrink_to_fit();
//         let bytes_ptr = bytes_array.as_mut_ptr();
//         let bytes_len = bytes_array.len();
//         mem::forget(bytes_array);
//         ret_ptr =  create_tuple_ptr(bytes_ptr as i64, bytes_len as i64);
//     });
//     ret_ptr
// }

#[no_mangle]
pub fn get_first_of_tuple(tuple_ptr: i64) -> i64 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).0
}

#[no_mangle]
pub fn get_second_of_tuple(tuple_ptr: i64) -> i64 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).1
}

#[no_mangle]
pub fn drop_tuple(tuple_ptr: i64) {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr);
    let tuple = tuple.deref();
    unsafe {
        drop(Vec::from_raw_parts(tuple.0 as *mut u8, tuple.1 as usize, tuple.1 as usize));
    };
}

#[no_mangle]
pub fn read_from_addr(mem_addr: i64) -> i32 {
    unsafe { std::ptr::read(mem_addr as *mut i32) }
}

#[no_mangle]
pub fn transform_ffi(batch: i64) -> i64 {
    let ffi_array = get_first_of_tuple(batch);
    let ffi_schema = get_second_of_tuple(batch);
    let array = unsafe { make_array_from_raw(ffi_array as *mut FFI_ArrowArray, ffi_schema as *mut FFI_ArrowSchema).unwrap() };
    let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    let input = RecordBatch::from(as_structarray);
    println!("input batch = {:?}", input);
    // let transformed_record = transform_record_batch(input);
    let transformed_record = input;
    println!("transformed record = {:?}", transformed_record);
    let struct_array: StructArray = transformed_record.into();
    let (out_array, out_schema) = struct_array.to_raw().unwrap();

    // let array = unsafe { make_array_from_raw(out_array as *mut FFI_ArrowArray, out_schema as *mut FFI_ArrowSchema).unwrap() };
    // let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    // let input = RecordBatch::from(as_structarray);
    // println!("input batch = {:?}", input);

    unsafe { println!("out schema wasm = {:?}", *out_schema); }

    let ret_ptr =  unsafe { create_tuple_ptr(out_array as i64, out_schema as i64) };
    ret_ptr
}

#[no_mangle]
pub fn transform_ffi2(ffi_array: i64, ffi_schema: i64) -> i64 {
    // let ffi_array = get_first_of_tuple(batch);
    // let ffi_schema = get_second_of_tuple(batch);
    println!("transform 2 ffi_array = {:?}, ffi_schema = {:?}", ffi_array, ffi_schema);
    let array = unsafe { make_array_from_raw(ffi_array as *mut FFI_ArrowArray, ffi_schema as *mut FFI_ArrowSchema).unwrap() };
    let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    let input = RecordBatch::from(as_structarray);
    println!("input batch = {:?}", input);
    // let transformed_record = transform_record_batch(input);
    let transformed_record = input;
    println!("transformed record = {:?}", transformed_record);
    let struct_array: StructArray = transformed_record.into();
    let (out_array, out_schema) = struct_array.to_raw().unwrap();

    // let array = unsafe { make_array_from_raw(out_array as *mut FFI_ArrowArray, out_schema as *mut FFI_ArrowSchema).unwrap() };
    // let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    // let input = RecordBatch::from(as_structarray);
    // println!("input batch = {:?}", input);

    unsafe { println!("out schema wasm = {:?}", *out_schema); }

    let ret_ptr =  unsafe { create_tuple_ptr(out_array as i64, out_schema as i64) };
    ret_ptr
}

#[no_mangle]
pub fn create_record_batch() -> i64 {
    // let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    // let schema = Schema::new(vec![
    // Field::new("id", DataType::Int64, false)
    // ]);

    // let batch = RecordBatch::try_new(
    // Arc::new(schema),
    // vec![Arc::new(id_array)]
    // ).unwrap();
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
    ).unwrap();

    // FFI c data interface
    // let struct_array: StructArray = batch.into();
    // let (in_array, in_schema) = struct_array.to_raw().unwrap();
    // let res = create_tuple_ptr(in_array as i64, in_schema as i64);
    // res as u64

    // IPC
    // Write the transformed record batch uing IPC
    let schema = batch.schema();
    let vec = Vec::new();
    let mut writer = crate::ipc::writer::StreamWriter::try_new(vec, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    let mut bytes_array = writer.into_inner().unwrap();
    bytes_array.shrink_to_fit();
    let bytes_ptr = bytes_array.as_mut_ptr();
    let bytes_len = bytes_array.len();
    mem::forget(bytes_array);
    let ret_ptr = create_tuple_ptr(bytes_ptr as i64, bytes_len as i64);
    ret_ptr
}

#[no_mangle]
pub fn transform_ipc(batch: i64) -> i64 {
    let bytes_ptr = get_first_of_tuple(batch);
    let bytes_len = get_second_of_tuple(batch);
    let bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts(bytes_ptr as *mut _, bytes_len as usize, bytes_len as usize) };
    let cursor = Cursor::new(bytes_array);
    let mut reader = StreamReader::try_new(cursor).unwrap();
    let mut ret_ptr = 0;
    // reader.for_each(|batch| {
        let batch = reader.next().unwrap();
        let batch = batch.unwrap();
        // Transform the record batch
        // let transformed = batch.clone();
        let transformed = transform_record_batch(batch);
        println!("transformed = {:?}", transformed);

        // Write the transformed record batch uing IPC
        let schema = transformed.schema();
        let vec = Vec::new();
        let mut writer = crate::ipc::writer::StreamWriter::try_new(vec, &schema).unwrap();
        writer.write(&transformed).unwrap();
        writer.finish().unwrap();
        let mut bytes_array = writer.into_inner().unwrap();
        // bytes_array.shrink_to_fit();
        let bytes_ptr = bytes_array.as_mut_ptr();
        let bytes_len = bytes_array.len();
        println!("transform ipc 2");
        mem::forget(bytes_array);
        ret_ptr = create_tuple_ptr(bytes_ptr as i64, bytes_len as i64);
        println!("transform ipc 3");
    // });
    mem::forget(reader);
    println!("transform ipc 4");
    ret_ptr
}