use core::ffi::CStr;
use pgrx::pg_sys::{
    jbvType_jbvArray as JBV_ARRAY, jbvType_jbvBinary as JBV_BINARY, jbvType_jbvBool as JBV_BOOL,
    jbvType_jbvDatetime as JBV_DATETIME, jbvType_jbvNull as JBV_NULL,
    jbvType_jbvNumeric as JBV_NUMERIC, jbvType_jbvObject as JBV_OBJECT,
    jbvType_jbvString as JBV_STRING, JsonbIteratorToken_WJB_BEGIN_ARRAY as WJB_BEGIN_ARRAY,
    JsonbIteratorToken_WJB_BEGIN_OBJECT as WJB_BEGIN_OBJECT,
    JsonbIteratorToken_WJB_DONE as WJB_DONE, JsonbIteratorToken_WJB_ELEM as WJB_ELEM,
    JsonbIteratorToken_WJB_END_ARRAY as WJB_END_ARRAY,
    JsonbIteratorToken_WJB_END_OBJECT as WJB_END_OBJECT, JsonbIteratorToken_WJB_KEY as WJB_KEY,
    JsonbIteratorToken_WJB_VALUE as WJB_VALUE,
    JsonbValue__bindgen_ty_1__bindgen_ty_1 as JsonbRawString,
    JsonbValue__bindgen_ty_1__bindgen_ty_2 as JsonbRawArray,
    JsonbValue__bindgen_ty_1__bindgen_ty_3 as JsonbRawObject,
};
use pgrx::prelude::*;
use pgrx::{direct_function_call, pg_sys};
use std::ffi::OsStr;
use std::fmt;
use std::os::unix::ffi::OsStrExt;
use std::str::FromStr as _;

pgrx::pg_module_magic!();

/// Step into nested JSONB objects or skip over them?
pub enum JsonbTraversal {
    SkipOver,
    StepInto,
}

/// An opaque reference to a string inside a JSONB document.
pub struct JsonbString<'a> {
    inner: &'a JsonbRawString,
}

/// An opaque reference to a numeric inside a JSONB document.
pub struct JsonbNumeric<'a> {
    inner: &'a pg_sys::Numeric,
}

/// An opaque reference to an array inside a JSONB document.
pub struct JsonbArray<'a> {
    inner: &'a JsonbRawArray,
}

/// An opaque reference to an object inside a JSONB document.
pub struct JsonbObject<'a> {
    inner: &'a JsonbRawObject,
}

/// A value inside a JSONB document.
pub enum JsonbValue<'a> {
    Null,
    String(JsonbString<'a>),
    Number(JsonbNumeric<'a>),
    Bool(bool),
}

pub trait JsonbVisitor<T, E> {
    fn begin_array(&mut self, num_elems: usize) -> Result<JsonbTraversal, E>;
    fn end_array(&mut self) -> Result<JsonbTraversal, E>;
    fn begin_object(&mut self, num_pairs: usize) -> Result<JsonbTraversal, E>;
    fn end_object(&mut self) -> Result<JsonbTraversal, E>;
    fn key(&mut self, val: JsonbValue) -> Result<JsonbTraversal, E>;
    fn value(&mut self, val: JsonbValue) -> Result<JsonbTraversal, E>;
    fn elem(&mut self, val: JsonbValue) -> Result<JsonbTraversal, E>;
    fn done(&mut self) -> Result<T, E>;
}

#[derive(Debug)]
enum SerdeValueBuilderState {
    Value(serde_json::Value),
    Array(Vec<serde_json::Value>),
    Object(serde_json::Map<String, serde_json::Value>, Option<String>),
}

#[derive(Default)]
struct SerdeValueBuilder {
    state_stack: Vec<SerdeValueBuilderState>,
}

impl SerdeValueBuilder {
    fn push_value(&mut self, out: serde_json::Value) {
        match self.state_stack.last_mut() {
            Some(SerdeValueBuilderState::Array(vec)) => {
                vec.push(out);
            }
            Some(SerdeValueBuilderState::Object(map, key)) => {
                let key = key.take().expect("Unexpected state: missing key");
                map.insert(key, out);
            }
            None => {
                self.state_stack.push(SerdeValueBuilderState::Value(out));
            }
            other => panic!("Unexpected state {other:?} while pushing value"),
        }
    }
}

impl JsonbVisitor<serde_json::Value, ()> for SerdeValueBuilder {
    fn begin_array(&mut self, num_elems: usize) -> Result<JsonbTraversal, ()> {
        self.state_stack
            .push(SerdeValueBuilderState::Array(Vec::with_capacity(num_elems)));
        Ok(JsonbTraversal::StepInto)
    }

    fn elem(&mut self, val: JsonbValue) -> Result<JsonbTraversal, ()> {
        match self.state_stack.last_mut() {
            Some(SerdeValueBuilderState::Array(ref mut vec)) => {
                vec.push(val.to_serde_json_value()?);
            }
            other => {
                panic!("Unexpected state {other:?} for elem");
            }
        }
        Ok(JsonbTraversal::StepInto)
    }

    fn end_array(&mut self) -> Result<JsonbTraversal, ()> {
        match self.state_stack.pop() {
            Some(SerdeValueBuilderState::Array(val)) => {
                self.push_value(serde_json::Value::Array(val));
            }
            other => {
                panic!("Unexpected state {other:?} for end_array");
            }
        }
        Ok(JsonbTraversal::StepInto)
    }

    fn begin_object(&mut self, num_pairs: usize) -> Result<JsonbTraversal, ()> {
        self.state_stack.push(SerdeValueBuilderState::Object(
            serde_json::Map::with_capacity(num_pairs),
            None,
        ));
        Ok(JsonbTraversal::StepInto)
    }

    fn key(&mut self, val: JsonbValue) -> Result<JsonbTraversal, ()> {
        let key = if let JsonbValue::String(str) = val {
            str.as_ref().to_os_string().into_string().or(Err(()))?
        } else {
            panic!("Non-String key");
        };

        if let Some(SerdeValueBuilderState::Object(map, None)) = self.state_stack.pop() {
            self.state_stack
                .push(SerdeValueBuilderState::Object(map, Some(key)));
        } else {
            panic!("Unexpected state");
        }
        Ok(JsonbTraversal::StepInto)
    }

    fn value(&mut self, val: JsonbValue) -> Result<JsonbTraversal, ()> {
        match self.state_stack.pop() {
            Some(SerdeValueBuilderState::Object(mut map, Some(key))) => {
                map.insert(key, val.to_serde_json_value()?);
                self.state_stack
                    .push(SerdeValueBuilderState::Object(map, None));
            }
            other => {
                panic!("Unexpected state {other:?} for value");
            }
        }
        Ok(JsonbTraversal::StepInto)
    }

    fn end_object(&mut self) -> Result<JsonbTraversal, ()> {
        match self.state_stack.pop() {
            Some(SerdeValueBuilderState::Object(map, None)) => {
                self.push_value(serde_json::Value::Object(map));
            }
            other => {
                panic!("Unexpected state {other:?} for end_object");
            }
        }
        Ok(JsonbTraversal::StepInto)
    }

    fn done(&mut self) -> Result<serde_json::Value, ()> {
        match self.state_stack.pop() {
            Some(SerdeValueBuilderState::Value(val)) => {
                assert!(self.state_stack.is_empty());
                Ok(val)
            }
            other => {
                panic!("Unexpected state {other:?} for done");
            }
        }
    }
}

impl<'a> AsRef<OsStr> for JsonbString<'a> {
    fn as_ref(&self) -> &OsStr {
        let slice = unsafe {
            std::slice::from_raw_parts(
                self.inner.val as *mut u8,
                self.inner
                    .len
                    .try_into()
                    .expect("i32 should fit into usize"),
            )
        };
        OsStr::from_bytes(slice)
    }
}

impl<'a> AsRef<CStr> for JsonbNumeric<'a> {
    fn as_ref(&self) -> &CStr {
        unsafe {
            direct_function_call::<&CStr>(
                pg_sys::numeric_out,
                &[Some(pg_sys::Datum::from(*self.inner))],
            )
        }
        .expect("should return a &CStr")
    }
}

impl<'a> JsonbValue<'a> {
    fn to_serde_json_value(&self) -> Result<serde_json::Value, ()> {
        Ok(match self {
            Self::Null => serde_json::Value::Null,
            Self::String(str) => {
                let str = str.as_ref().to_os_string().into_string().or(Err(()))?;
                serde_json::Value::String(str)
            }
            Self::Number(str) => serde_json::Value::Number(
                // Safe to assume PostgreSQL doesn't emit unicode in numerics
                serde_json::value::Number::from_str(unsafe {
                    std::str::from_utf8_unchecked(str.as_ref().to_bytes())
                })
                .or(Err(()))?,
            ),
            Self::Bool(val) => serde_json::Value::Bool(*val),
        })
    }
}

impl<'a> fmt::Display for JsonbValue<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_serde_json_value().or(Err(fmt::Error))?)
    }
}

impl<'a> JsonbArray<'a> {
    pub fn len(&self) -> usize {
        self.inner
            .nElems
            .try_into()
            .expect("i32 should fit into usize")
    }

    pub fn is_empty(&self) -> bool {
        self.inner.nElems == 0
    }
}

pub struct JsonbPair<'a>(&'a pg_sys::JsonbPair);

impl<'a> JsonbObject<'a> {
    pub fn len(&self) -> usize {
        self.inner
            .nPairs
            .try_into()
            .expect("i32 should fit into usize")
    }

    pub fn is_empty(&self) -> bool {
        self.inner.nPairs == 0
    }
}

impl<'a> JsonbValue<'a> {
    fn from_pg_sys(val: &'a pg_sys::JsonbValue) -> Self {
        match val.type_ {
            JBV_NULL => Self::Null,
            JBV_STRING => Self::String(JsonbString {
                inner: unsafe { &val.val.string },
            }),
            JBV_NUMERIC => Self::Number(JsonbNumeric {
                inner: unsafe { &val.val.numeric },
            }),
            JBV_BOOL => Self::Bool(unsafe { val.val.boolean }),
            JBV_DATETIME => todo!("datetime support"),
            _ => panic!("Unknown JsonValue type"),
        }
    }
}

/// Iterate over the `jsonb` document, feeding results into the given
/// `visitor`.
///
/// # Safety
///
/// Currently marked unsafe because it accepts a raw pointer and
/// operates on it. Caller is currently responsible for passing
/// in a correct pointer. This should be fixed by changing them
/// argument type to some Datum.
pub unsafe fn iterate_jsonb<V, E, S: JsonbVisitor<V, E>>(
    jsonb: *mut pg_sys::Jsonb,
    mut visitor: S,
    skip_root: JsonbTraversal,
) -> Result<V, E> {
    let mut it = pg_sys::JsonbIteratorInit(&mut (*jsonb).root);
    let result = {
        let mut val = pg_sys::JsonbValue::default();
        let mut skip = skip_root;

        loop {
            let token = unsafe {
                pg_sys::JsonbIteratorNext(
                    &mut it,
                    &mut val,
                    match skip {
                        JsonbTraversal::StepInto => false,
                        JsonbTraversal::SkipOver => true,
                    },
                )
            };
            skip = match token {
                WJB_DONE => {
                    break visitor.done();
                }
                WJB_KEY => visitor.key(JsonbValue::from_pg_sys(&val))?,
                token if matches!(token, WJB_VALUE | WJB_ELEM) => {
                    if val.type_ == JBV_BINARY {
                        todo!("binary handling");
                        // Not yet sure what to do here:
                        // - it doesn't seem like we can access the array/object in bulk
                        // - we can report skipped_array/skipped_object with element/pair count to the visitor?
                    } else {
                        let value = JsonbValue::from_pg_sys(&val);
                        if token == WJB_VALUE {
                            visitor.value(value)?
                        } else {
                            visitor.elem(value)?
                        }
                    }
                }
                WJB_BEGIN_ARRAY => {
                    assert!(val.type_ == JBV_ARRAY);
                    visitor.begin_array(
                        val.val
                            .array
                            .nElems
                            .try_into()
                            .expect("i32 should fit into usize"),
                    )?
                }
                WJB_END_ARRAY => visitor.end_array()?,
                WJB_BEGIN_OBJECT => {
                    assert!(val.type_ == JBV_OBJECT);
                    visitor.begin_object(
                        val.val
                            .object
                            .nPairs
                            .try_into()
                            .expect("i32 should fit into usize"),
                    )?
                }
                WJB_END_OBJECT => visitor.end_object()?,
                _ => panic!("Invalid iterator state"),
            };
        }
    };

    result
}

#[pg_extern(sql = r#"
    CREATE FUNCTION "jsonb_test"(jsonb) RETURNS boolean
    STRICT
    LANGUAGE c /* Rust */
    AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
fn jsonb_test(datum: pg_sys::Datum) -> bool {
    let varlena = datum.cast_mut_ptr();
    let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(varlena) };

    let result = unsafe {
        iterate_jsonb(
            detoasted as *mut pg_sys::Jsonb,
            SerdeValueBuilder::default(),
            JsonbTraversal::SkipOver,
        )
        .unwrap()
    };

    if detoasted != varlena {
        unsafe {
            pg_sys::pfree(detoasted as pgrx::void_mut_ptr);
        }
    }

    // Ensure nothing gets optimized away
    matches!(result, serde_json::Value::Object(_))
}

#[pg_extern(sql = r#"
    CREATE FUNCTION "jsonb_to_text"(jsonb) RETURNS text
    STRICT
    LANGUAGE c /* Rust */
    AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
fn jsonb_to_text(datum: pg_sys::Datum) -> Option<String> {
    let varlena = datum.cast_mut_ptr();
    let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(varlena) };

    let result = unsafe {
        iterate_jsonb(
            detoasted as *mut pg_sys::Jsonb,
            SerdeValueBuilder::default(),
            JsonbTraversal::SkipOver,
        )
        .unwrap()
    };

    if detoasted != varlena {
        unsafe {
            pg_sys::pfree(detoasted as pgrx::void_mut_ptr);
        }
    }

    // Ensure nothing gets optimized away
    Some(serde_json::to_string(&result).expect("should be able to print serde_json::Value"))
}

#[pg_extern]
fn jsonb_test2(datum: pgrx::JsonB) -> bool {
    // Ensure nothing gets optimized away
    matches!(datum.0, serde_json::Value::Object(_))
}
