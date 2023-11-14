use core::ffi::CStr;
use ouroboros::self_referencing;
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
    inner: JsonbRawString,
    _covariant: std::marker::PhantomData<&'a ()>,
}

/// An opaque reference to a numeric inside a JSONB document.
pub struct JsonbNumeric<'a> {
    inner: pg_sys::Numeric,
    _covariant: std::marker::PhantomData<&'a ()>,
}

/// A value inside a JSONB document.
pub enum JsonbScalar<'a> {
    Null,
    String(JsonbString<'a>),
    Number(JsonbNumeric<'a>),
    Bool(bool),
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
                &[Some(pg_sys::Datum::from(self.inner))],
            )
        }
        .expect("should return a &CStr")
    }
}

impl<'a> JsonbScalar<'a> {
    fn to_serde_json_value(&self) -> Result<serde_json::Value, ()> {
        Ok(match self {
            Self::Null => serde_json::Value::Null,
            Self::String(str) => {
                let str = str.as_ref().to_os_string().into_string().or(Err(()))?;
                serde_json::Value::String(str)
            }
            Self::Number(str) => serde_json::Value::Number(
                // Safe to assume PostgreSQL doesn't emit non-ASCII7 in numerics
                serde_json::value::Number::from_str(unsafe {
                    std::str::from_utf8_unchecked(str.as_ref().to_bytes())
                })
                .or(Err(()))?,
            ),
            Self::Bool(val) => serde_json::Value::Bool(*val),
        })
    }
}

impl<'a> fmt::Display for JsonbScalar<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_serde_json_value().or(Err(fmt::Error))?)
    }
}

impl<'a> JsonbScalar<'a> {
    fn from_pg_sys(val: pg_sys::JsonbValue) -> Self {
        match val.type_ {
            JBV_NULL => Self::Null,
            JBV_STRING => Self::String(JsonbString {
                inner: unsafe { val.val.string },
                _covariant: std::marker::PhantomData,
            }),
            JBV_NUMERIC => Self::Number(JsonbNumeric {
                inner: unsafe { val.val.numeric },
                _covariant: std::marker::PhantomData,
            }),
            JBV_BOOL => Self::Bool(unsafe { val.val.boolean }),
            JBV_DATETIME => todo!("datetime support"),
            _ => panic!("Unknown JsonValue type"),
        }
    }
}

struct JsonbBlob<'a> {
    ptr: *mut pg_sys::Jsonb,
    owned: bool,
    _covariant: std::marker::PhantomData<&'a ()>,
}

impl<'a> Drop for JsonbBlob<'a> {
    fn drop(&mut self) {
        if self.owned {
            unsafe {
                pg_sys::pfree(self.ptr.cast());
            }
        }
    }
}

impl<'a> JsonbBlob<'a> {
    fn new(varlena: *mut pg_sys::varlena) -> Self {
        let ptr = unsafe { pg_sys::pg_detoast_datum_packed(varlena) };
        Self {
            ptr: ptr as *mut pg_sys::Jsonb,
            owned: ptr != varlena,
            _covariant: std::marker::PhantomData,
        }
    }
}

struct JsonbIteratorRef<'a> {
    ptr: *mut pg_sys::JsonbIterator,
    _covariant: std::marker::PhantomData<&'a ()>,
}

// impl<'a> Drop for JsonbIteratorRef<'a> {
//     fn drop(&mut self) {
//         unsafe {
//             pg_sys::pfree(self.ptr.cast());
//         }
//     }
// }

#[self_referencing]
struct JsonbIteratorInner<'a> {
    detoasted: JsonbBlob<'a>,
    #[borrows(mut detoasted)]
    #[covariant]
    it: JsonbIteratorRef<'this>,
}

struct JsonbIterator<'a>(JsonbIteratorInner<'a>);

enum JsonbItem<'a> {
    BeginArray(usize),
    EndArray,
    BeginObject(usize),
    Value(JsonbScalar<'a>),
    EndObject,
}

impl<'a> JsonbIterator<'a> {
    fn new(detoasted: JsonbBlob<'a>) -> Self {
        Self(
            JsonbIteratorInnerBuilder {
                detoasted,
                it_builder: |detoasted: &'_ mut JsonbBlob<'_>| unsafe {
                    JsonbIteratorRef {
                        ptr: pg_sys::JsonbIteratorInit(&mut (*detoasted.ptr).root),
                        _covariant: std::marker::PhantomData,
                    }
                },
            }
            .build(),
        )
    }

    fn next<'b>(&'b mut self, traversal: JsonbTraversal) -> Option<JsonbItem<'b>> {
        let mut val = pg_sys::JsonbValue::default();
        let token = self.0.with_mut(|fields| unsafe {
            pg_sys::JsonbIteratorNext(
                &mut fields.it.ptr,
                &mut val,
                match traversal {
                    JsonbTraversal::StepInto => false,
                    JsonbTraversal::SkipOver => true,
                },
            )
        });
        match token {
            WJB_DONE => None,
            token if matches!(token, WJB_VALUE | WJB_ELEM | WJB_KEY) => {
                if val.type_ == JBV_BINARY {
                    todo!("binary handling");
                    // Not yet sure what to do here:
                    // - it doesn't seem like we can access the array/object in bulk
                    // - we can report skipped_array/skipped_object with element/pair count to the visitor?
                } else {
                    Some(JsonbItem::Value(JsonbScalar::from_pg_sys(val)))
                }
            }
            WJB_BEGIN_ARRAY => {
                assert!(val.type_ == JBV_ARRAY);
                Some(JsonbItem::BeginArray(unsafe {
                    val.val
                        .array
                        .nElems
                        .try_into()
                        .expect("i32 should fit into usize")
                }))
            }
            WJB_END_ARRAY => Some(JsonbItem::EndArray),
            WJB_BEGIN_OBJECT => {
                assert!(val.type_ == JBV_OBJECT);
                Some(JsonbItem::BeginObject(unsafe {
                    val.val
                        .object
                        .nPairs
                        .try_into()
                        .expect("i32 should fit into usize")
                }))
            }
            WJB_END_OBJECT => Some(JsonbItem::EndObject),
            _ => panic!("Invalid iterator state"),
        }
    }
}

fn read_value(it: &mut JsonbIterator) -> serde_json::Value {
    match it.next(JsonbTraversal::StepInto) {
        Some(JsonbItem::Value(val)) => val.to_serde_json_value().unwrap(),
        Some(JsonbItem::BeginObject(num_elems)) => {
            let mut map = serde_json::Map::with_capacity(num_elems);
            loop {
                let key = it.next(JsonbTraversal::StepInto);
                match key {
                    Some(JsonbItem::Value(JsonbScalar::String(key))) => {
                        let key = key
                            .as_ref()
                            .to_os_string()
                            .into_string()
                            .or(Err(()))
                            .unwrap();
                        map.insert(key, read_value(it));
                    }
                    Some(JsonbItem::EndObject) => {
                        return serde_json::Value::Object(map);
                    }
                    _ => panic!(),
                }
            }
        }
        Some(JsonbItem::BeginArray(num_elems)) => {
            let mut vec = Vec::with_capacity(num_elems);
            for _ in 0..num_elems {
                vec.push(read_value(it))
            }
            assert!(matches!(
                it.next(JsonbTraversal::StepInto),
                Some(JsonbItem::EndArray)
            ));
            return serde_json::Value::Array(vec);
        }
        _ => panic!(),
    }
}

#[pg_extern(sql = r#"
    CREATE FUNCTION "jsonb_to_text"(jsonb) RETURNS text
    STRICT
    LANGUAGE c /* Rust */
    AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
fn jsonb_to_text(datum: pg_sys::Datum) -> Option<String> {
    let blob = JsonbBlob::new(datum.cast_mut_ptr());
    let mut it = JsonbIterator::new(blob);
    let value = read_value(&mut it);

    // Ensure nothing gets optimized away
    Some(serde_json::to_string(&value).expect("should be able to print serde_json::Value"))
}

#[pg_extern]
fn jsonb_test2(datum: pgrx::JsonB) -> bool {
    // Ensure nothing gets optimized away
    matches!(datum.0, serde_json::Value::Object(_))
}

#[pg_extern(sql = r#"
    CREATE FUNCTION "jsonb_test"(jsonb) RETURNS boolean
    STRICT
    LANGUAGE c /* Rust */
    AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
fn jsonb_test3(datum: pg_sys::Datum) -> bool {
    let blob = JsonbBlob::new(datum.cast_mut_ptr());
    let mut it = JsonbIterator::new(blob);
    let value = read_value(&mut it);

    // Ensure nothing gets optimized away
    matches!(value, serde_json::Value::Object(_))
}
