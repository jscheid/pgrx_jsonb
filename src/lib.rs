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
    JsonbValue__bindgen_ty_1__bindgen_ty_4 as JsonbRawBinary,
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

/// An opaque reference to a binary inside a JSONB document.
pub struct JsonbBinary<'a> {
    inner: &'a JsonbRawBinary,
}

/// A value inside a JSONB document.
pub enum JsonbValue<'a> {
    Null,
    String(JsonbString<'a>),
    Number(JsonbNumeric<'a>),
    Bool(bool),
    Array(JsonbArray<'a>),
    Object(JsonbObject<'a>),
    Binary(JsonbBinary<'a>),
}

pub trait JsonbVisitor<T, E> {
    fn begin_array(&mut self) -> Result<JsonbTraversal, E>;
    fn end_array(&mut self) -> Result<JsonbTraversal, E>;
    fn begin_object(&mut self) -> Result<JsonbTraversal, E>;
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
    fn begin_array(&mut self) -> Result<JsonbTraversal, ()> {
        self.state_stack.push(SerdeValueBuilderState::Array(vec![]));
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
                //val.shrink_to_fit();
                self.push_value(serde_json::Value::Array(val));
            }
            other => {
                panic!("Unexpected state {other:?} for end_array");
            }
        }
        Ok(JsonbTraversal::StepInto)
    }

    fn begin_object(&mut self) -> Result<JsonbTraversal, ()> {
        self.state_stack
            .push(SerdeValueBuilderState::Object(serde_json::Map::new(), None));
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
                // FIXME: compact map here?
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
            std::slice::from_raw_parts(self.inner.val as *mut u8, self.inner.len as usize)
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
                serde_json::value::Number::from_str(str.as_ref().to_str().or(Err(()))?)
                    .or(Err(()))?,
            ),
            Self::Bool(val) => serde_json::Value::Bool(*val),
            Self::Array(val) => serde_json::Value::Array(
                val.iter()
                    .map(|v| v.to_serde_json_value())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Object(val) => serde_json::Value::Object(
                val.iter()
                    .map(|(key, value)| Ok((key.to_string(), value.to_serde_json_value()?)))
                    .collect::<Result<serde_json::Map<_, _>, _>>()?,
            ),
            Self::Binary(_val) => todo!(),
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

    pub fn iter(
        &self,
    ) -> std::iter::Map<
        std::slice::Iter<'_, pg_sys::JsonbValue>,
        impl FnMut(&'a pgrx::pg_sys::JsonbValue) -> JsonbValue<'a>,
    > {
        unsafe { std::slice::from_raw_parts(self.inner.elems, self.inner.nElems as usize) }
            .iter()
            .map(JsonbValue::from_pg_sys)
    }
}

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

    pub fn iter(
        &self,
    ) -> std::iter::Map<
        std::slice::Iter<'_, pg_sys::JsonbPair>,
        impl FnMut(&'a pgrx::pg_sys::JsonbPair) -> (JsonbValue<'a>, JsonbValue<'a>),
    > {
        unsafe { std::slice::from_raw_parts(self.inner.pairs, self.inner.nPairs as usize) }
            .iter()
            .map(
                |pg_sys::JsonbPair {
                     key,
                     value,
                     order: _order,
                 }| {
                    (JsonbValue::from_pg_sys(key), JsonbValue::from_pg_sys(value))
                },
            )
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
            JBV_ARRAY => Self::Array(JsonbArray {
                inner: unsafe { &val.val.array },
            }),
            JBV_OBJECT => Self::Object(JsonbObject {
                inner: unsafe { &val.val.object },
            }),
            JBV_BINARY => Self::Binary(JsonbBinary {
                inner: unsafe { &val.val.binary },
            }),
            JBV_DATETIME => todo!(),
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
) -> Result<V, E> {
    let mut it = pg_sys::JsonbIteratorInit(&mut (*jsonb).root);
    let mut val = pg_sys::JsonbValue::default();
    let mut skip = JsonbTraversal::StepInto; // FIXME: should be configurable

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
            WJB_DONE => return visitor.done(),
            WJB_KEY => visitor.key(JsonbValue::from_pg_sys(&val))?,
            WJB_VALUE => visitor.value(JsonbValue::from_pg_sys(&val))?,
            WJB_ELEM => visitor.elem(JsonbValue::from_pg_sys(&val))?,
            WJB_BEGIN_ARRAY => visitor.begin_array()?,
            WJB_END_ARRAY => visitor.end_array()?,
            WJB_BEGIN_OBJECT => visitor.begin_object()?,
            WJB_END_OBJECT => visitor.end_object()?,
            _ => panic!("Invalid iterator state"),
        };
    }
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

#[pg_extern]
fn jsonb_test2(datum: pgrx::JsonB) -> bool {
    // Ensure nothing gets optimized away
    matches!(datum.0, serde_json::Value::Object(_))
}
