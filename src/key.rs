use serde::Serialize;
use serde_json::to_string;

use crate::{SortOrder, util::{Gt, Gte, Eq, Exists, Like, Neq}};

/// This can be used for filters or getting fields
pub trait Key {
	/// Produces the string that will be used by SQL.
	fn key(&self, data_key: &str) -> String;

	/// Compares for equality.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is equal to 3
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").eq(3)).field("number", &connection)?;
	/// // Only 2 entries should've matched the filter
	/// assert_eq!(numbers.len(), 2);
	/// // They both should be equal to 3
	/// assert_eq!(numbers.into_iter().any(|number| number != 3), false);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn eq<T: Serialize>(self, value: T) -> Eq<Self, String>
		where Self: Sized {
		Eq { variable: self, value: to_string(&value).unwrap() }
	}

	/// Compares for inequality.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is not equal to 3
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").neq(3)).field("number", &connection)?;
	/// // 3 entries should've matched the filter
	/// assert_eq!(numbers.len(), 3);
	/// // They both should be equal to 3
	/// assert!(!numbers.into_iter().any(|number| number == 3));
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn neq<T: Serialize>(self, value: T) -> Neq<Self, String>
		where Self: Sized {
		Neq { variable: self, value: to_string(&value).unwrap() }
	}
	/// Compares if it is greater than the value.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is greater than 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").gt(4)).field("number", &connection)?;
	/// // Only 9 is bigger than 4
	/// assert_eq!(numbers.len(), 1);
	/// assert!(numbers[0] > 4);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn gt<T: Serialize>(self, value: T) -> Gt<Self, String>
		where Self: Sized{
		Gt { greater: self, lesser: to_string(&value).unwrap() }
	}
	/// Compares if it is greater than or equal to the value.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is greater than or equal to 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").gte(4)).field("number", &connection)?;
	/// assert_eq!(numbers.len(), 2);
	/// assert!(!numbers.into_iter().any(|number| number < 4));
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn gte<T: Serialize>(self, value: T) -> Gte<Self, String>
		where Self: Sized {
		Gte { greater: self, lesser: to_string(&value).unwrap() }
	}
	/// Compares if it is less than the value.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is less than 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").lt(4)).field("number", &connection)?;
	/// assert_eq!(numbers.len(), 3);
	/// assert!(!numbers.into_iter().any(|number| number >= 4));
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn lt<T: Serialize>(self, value: T) -> Gt<String, Self>
		where Self: Sized {
		Gt { lesser: self, greater: to_string(&value).unwrap() }
	}
	/// Compares if it is greater than or equal to the variable.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is less than or equal to 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").lte(4)).field("number", &connection)?;
	/// assert_eq!(numbers.len(), 4);
	/// assert!(!numbers.into_iter().any(|number| number > 4));
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn lte<T: Serialize>(self, value: T) -> Gte<String, Self>
		where Self: Sized {
		Gte { lesser: self, greater: to_string(&value).unwrap() }
	}
	/// Uses the SQL like comparison operator.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"name": "Hiruna"}), &connection)?;
	/// table.insert(json!({"name": "Alex"}), &connection)?;
	/// table.insert(json!({"name": "Bob"}), &connection)?;
	/// table.insert(json!({"name": "Haruna"}), &connection)?;
	/// table.insert(json!({"name": "Felix"}), &connection)?;
	///
	/// // Get all names that end with 'x'
	/// let names: Vec<String> = table.iter()
	/// 	.filter(field("name").like(true, 'x', false)).field("name", &connection)?;
	/// // We should only match two names
	/// assert_eq!(names.len(), 2);
	///
	/// // Get all names that start with `H` and end with 'runa'
	/// let names: Vec<String> = table.iter()
	/// 	.filter(field("name").like(false, "H%runa", false)).field("name", &connection)?;
	/// // We should only match two names
	/// assert_eq!(names.len(), 2);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn like<S: std::fmt::Display>(self, matches_start: bool, value: S, matches_end: bool) -> Like<Self, S>
		where Self: Sized {
		Like { variable: self, matches_start, value, matches_end }
	}

	/// Whether the value exists in the JSON object and if it does exist, whether it is not null.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 3, "b": 20}), &connection)?;
	/// table.insert(json!({"b": 3, "c": 20}), &connection)?;
	/// table.insert(json!({"a": 89, "b": 3, "c": 20}), &connection)?;
	/// table.insert(json!({"a": null, "b": 3, "c": 20}), &connection)?;
	///
	/// let ids: Vec<i64> = table.iter().filter(field("a").exists()).id(&connection)?;
	/// // Only Entry 1 and 3 should have passed the filter
	/// // Entry 2 doesn't have the `a` field and Entry 4 has an `a` field but it's `null`
	/// // Therefore only two entries should have passed
	/// assert_eq!(ids.len(), 2);
	/// assert_eq!(ids[0], 1);
	/// assert_eq!(ids[1], 3);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn exists(self) -> Exists<Self> where Self: Sized { Exists(self) }

	/// This field is to be sorted in ascending order.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 4}), &connection)?;
	/// table.insert(json!({"a": 8}), &connection)?;
	/// table.insert(json!({"a": 6}), &connection)?;
	///
	/// let a: Vec<u8> = table.iter().sort(field("a").ascending()).field("a", &connection)?;
	///
	/// assert_eq!(a[0], 4);
	/// assert_eq!(a[1], 6);
	/// assert_eq!(a[2], 8);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn ascending(self) -> SortOrder<Self> where Self: Sized { SortOrder::Ascending(self) }

	/// This field is to be sorted in descending order.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 4}), &connection)?;
	/// table.insert(json!({"a": 8}), &connection)?;
	/// table.insert(json!({"a": 6}), &connection)?;
	///
	/// let a: Vec<u8> = table.iter().sort(field("a").descending()).field("a", &connection)?;
	///
	/// assert_eq!(a[0], 8);
	/// assert_eq!(a[1], 6);
	/// assert_eq!(a[2], 4);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	fn descending(self) -> SortOrder<Self> where Self: Sized { SortOrder::Descending(self) }
}
impl<K: Key + ?Sized> Key for &K {
	fn key(&self, data_key: &str) -> String { (*self).key(data_key) }
}

/// Formats the JSON field key into a path so that it can be used with the extension.
///
/// # Example
///
/// ```
/// # use nosqlite::format_key;
/// assert_eq!(format_key("x"), "$.x");
/// assert_eq!(format_key("[0]"), "$[0]");
/// assert_eq!(format_key(".x"), "$.x");
/// ```
pub fn format_key(key: &str) -> String {
	let mut chars = key.chars();
	let mut prepend = String::with_capacity(2+key.len());
	let char1 = chars.next();
	if char1 != Some('$') {
		prepend.push('$');
		let c = chars.next();
		if char1 != Some('.') && char1 != Some('[') && (c.is_some() || key.len() == 1) {
			prepend.push('.');
		}
	}
	prepend.push_str(key);
	prepend
}


/// Represents a field in a JSON object.
///
/// Create this using the [`field`] method.
///
/// [`field`]: fn.field.html
pub struct Field(pub String);
/// Creates a representation of a field in a JSON object.
///
/// If the string is empty, the root is assumed.
pub fn field(field: &str) -> Field { Field(format_key(field)) }
impl Key for Field {
	fn key(&self, data_key: &str) -> String {
		format!("json_extract({}, \"{}\")", data_key, self.0)
	}
}

/// A column in the SQL table.
///
/// Create this using the [`column`] function.
///
/// This isn't generally used unless you have a custom table.
///
/// # Example
///
/// ```
/// # use nosqlite::{column, Column, Connection, json, Key, Table};
/// # let connection = Connection::in_memory()?;
/// # let table = connection.table("test")?;
/// table.insert(10, &connection)?;
/// table.insert(20, &connection)?;
/// table.insert(30, &connection)?;
/// // Get the entries with an id greater than 1
/// let data = table.iter().filter(column("id").gt(1)).id(&connection)?;
/// // Only 2 entries should have been queried
/// assert_eq!(data.len(), 2);
/// assert_eq!(data.into_iter().any(|id| id == 1), false);
/// # Ok::<(), rusqlite::Error>(())

/// ```
///
/// [`column`]: fn.column.html
pub struct Column(pub String);
/// Create a representation of a column in a SQL table.
pub fn column<S: Into<String>>(column: S) -> Column { Column(column.into()) }
impl Key for Column {
	fn key(&self, _: &str) -> String { self.0.clone() }
}
