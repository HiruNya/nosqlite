use rusqlite::{Connection as SqliteConnection, NO_PARAMS, OptionalExtension, Result as SqliteResult,
				types::{FromSql, ToSql}};
use serde::{de::DeserializeOwned, Serialize};

use std::{fmt::Display, marker::PhantomData};

use crate::{Entry, format_key, Iterator, Json, Key};

/// A table in the database.
///
/// Use [`Connection::table`] to make this struct but if you have a custom schema,
/// you should make this struct by providing the fields your self.
///
/// The generic type parameter, I, is the type of the Id column.
pub struct Table<I> {
	/// The id column.
	///
	/// The default implementation uses an integer but the value does not matter.
	/// e.g. a String UUID can be used.
	pub id: String,
	pub(crate) id_type: PhantomData<fn() -> I>, // The type of the id.
	/// The column that stores the JSON object.
	///
	/// The default implementation uses a Non Null Text but the value can be nullable or a Blob.
	pub data: String,
	/// The name of the table.
	pub name: String,
}
impl<A> Table<A> {
	/// Creates an index on the table with the given keys.
	///
	/// This is meant to speed up queries but whether it is actually used or not is determined
	/// by SQLite at runtime so I would suggest running some sort of benchmark to see if creating
	/// the index actually does speed up query times.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{column, Connection, field};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.index("my_index", &[field("name"), field("age")], connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	///
	/// If you want to index both a field and a column then you need to cast the reference
	/// to a `&dyn Key`.
	/// ```
	/// # use nosqlite::{column, Connection, field, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.index("my_index", &[&field("name") as &dyn Key, &column("id") as &dyn Key], connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn index<S, I, T, C>(&self, name: S, keys: I, connection: C) -> SqliteResult<()>
		where
			S: Display,
			I: IntoIterator<Item=T>,
			T: Key,
			C: AsRef<SqliteConnection>,
	{
		let keys = keys.into_iter().map(|k| k.key(&self.data))
			.fold(String::new(), |mut s, k| {
				if !s.is_empty() {
					s.push(',');
				}
				s.push_str(k.as_str());
				s
			});
		connection.as_ref().prepare(&format!("CREATE INDEX {} ON {} ({})", name, self.name, keys))?
			.execute(NO_PARAMS).map(|_|())
	}
}
impl<I: FromSql> Table<I> {
	/// Creates a table but doesn't check if the table exists.
	///
	/// This can be used to give a custom name and type of the id and data columns.
	///
	/// Generally using [`Connection::table`] is recommended instead of this.
	pub fn unchecked<K: FromSql, T: Into<String>>(id: T, data: T, name: T) -> Self {
		Self {
			id: id.into(),
			data: data.into(),
			name: name.into(),
			id_type: PhantomData::default(),
		}
	}

	/// Iterate through all the entries in the table.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// let table = connection.table("people")?;
	/// table.insert(json!({"name": "Hiruna", "age": 19}), &connection)?;
	/// table.insert(json!({"name": "Bobby", "age": 13}), &connection)?;
	/// let data = table.iter()
	/// 	.filter(field("age").gte(18))
	/// 	.fields::<(String, u8), _, _, _>(&["name", "age"], &connection)?;
	/// assert_eq!(data.len(), 1);
	/// assert!(data[0].1 > 18);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn iter(&self) -> Iterator<I, (), ()> {
		Iterator {
			data_key: &self.data,
			id_key: &self.id,
			id_type: self.id_type,
			limit: None,
			offset: None,
			order_by: (),
			table_key: &self.name,
			where_: (),
		}
	}

	/// Inserts a JSON object into the data column of the table.
	///
	/// Multiple JSON objects that are exactly the same can be inserted.
	///
	/// **Warning**: If your table has other columns that are not nullable, then you should not use this.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, json, Table};
	/// # let connection = Connection::in_memory()?;
	/// // This table is empty
	/// let table = connection.table("people")?;
	/// // We'll insert two JSON objects which are exactly the same
	/// let json = json!({"name": "Hiruna", "age": 19});
	/// table.insert(&json, &connection)?;
	/// table.insert(&json, &connection)?;
	/// // Now we'll check how many entries the table has
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 2);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn insert<T: Serialize, C: AsRef<SqliteConnection>>(&self, data: T, connection: C) -> SqliteResult<()> {
		connection.as_ref().prepare(&format!("INSERT INTO {} ({}) VALUES (?)", self.name, self.data))?
			.execute(&[&Json(data)])?;
		Ok(())
	}
}
impl <I: FromSql + ToSql> Table<I> {
	/// Gets a JSON object using a id from the id column.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, json, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // The table of these ids are integers
	/// let table = connection.table("people")?;
	/// table.insert(&Person {name: "Hiruna".into()}, &connection)?;
	/// table.insert(&Person {name: "Bobby".into()}, &connection)?;
	/// // Now we'll get the 2nd entry from the table.
	/// let bobby: Person = table.get(2).data(&connection)?.unwrap();
	/// assert_eq!(bobby.name, "Bobby");
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn get(&self, id: I) -> Operation<I> {
		Operation { id, data_key: &self.data, id_key: &self.id, table: &self.name }
	}

	/// Deletes an entry with the given primary key.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, json, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// // We insert 2 entries
	/// table.insert(json!(1), &connection)?;
	/// table.insert(json!(2), &connection)?;
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 2);
	/// // Remove the entry with id 1
	/// table.delete(1, &connection)?;
	/// // Table should only have entry now
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 1);
	/// // And we shouldn't be able to access the entry with primary key 1 now
	/// assert!(table.get(1).id(&connection)?.is_none());
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn delete<C: AsRef<SqliteConnection>>(&self, id: I, connection: C) -> SqliteResult<()> {
		connection.as_ref().execute(
			&format!("DELETE FROM {} WHERE {} = ?", self.name, self.id),
			&[&id],
		).map(|_|())
	}
}

/// Represents an operation to get a JSON object using its id key.
#[must_use = "This struct must be used for the database to be queried."]
pub struct Operation<'a, I: FromSql + ToSql> {
	data_key: &'a str,
	id: I,
	id_key: &'a str,
	table: &'a str,
}
impl<'a, I: FromSql + ToSql> Operation<'a, I> {
	/// Gets only the JSON object, deserialising it into the struct provided.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby: Person = table.get(2).data(&connection)?.unwrap();
	/// assert_eq!(bobby.name, "Bobby");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn data<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<T>> {
		connection.as_ref().query_row(
			&format!("SELECT {} FROM {} WHERE {} = ?", self.data_key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).map(Json::unwrap).optional()
	}
	/// Gets both the id and the JSON object.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby: Entry<i64, Person> = table.get(2).entry(&connection)?.unwrap();
	/// assert_eq!(bobby.data.name, "Bobby");
	/// assert_eq!(bobby.id, 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<Entry<I, T>>> {
		connection.as_ref().query_row(
			&format!("SELECT {}, {} FROM {} WHERE {} = ?", self.id_key, self.data_key, self.table, self.id_key),
			&[&self.id],
			Entry::from_row
		).optional()
	}
	/// Gets only the id of the entry.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby_id: i64 = table.get(2).id(&connection)?.unwrap();
	/// assert_eq!(bobby_id, 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn id<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<I>> {
		connection.as_ref().query_row(
			&format!("SELECT {} FROM {} WHERE {} = ?", self.id_key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}
	/// Extracts a possibly nested field in the JSON object.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby: String = table.get(2).field("name", &connection)?.unwrap();
	/// assert_eq!(bobby, "Bobby");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn field<T: FromSql, C: AsRef<SqliteConnection>>(&self, key: &str, connection: C) -> SqliteResult<Option<T>> {
		let key = format_key(key);
		connection.as_ref().query_row(
			&format!("SELECT json_extract({}, \"{}\") FROM {} WHERE {} = ?", self.data_key, key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}

	/// Removes a *field* from a JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({ "name": "Hiruna", "age": 19 }), &connection)?;
	/// table.insert(json!({ "name": "Bob", "age": 13 }), &connection)?;
	///
	/// table.get(2).remove("age", &connection);
	/// // Only 1 entry is returned because the second JSON object doesn't have an `age` field
	/// // so only entry 1 has an age field
	/// let people: Vec<(String, u8)> = table.iter().fields(&["name", "age"], &connection)?;
	/// assert_eq!(people.len(), 1);
	/// assert_eq!(people[0].0, "Hiruna");
	/// // This *does not* delete the entry
	/// assert_eq!(table.iter().id(&connection)?.len(), 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn remove<C>(&self, field: &str, connection: C) -> SqliteResult<()>
		where C: AsRef<SqliteConnection>
	{
		let path = format_key(field);
		let set_value = format!("{} = json_remove({}, '{}')", self.data_key, self.data_key, path);
		connection.as_ref().execute(
			&format!("UPDATE {} SET {} WHERE {} = ?", self.table, set_value, self.id_key),
			&[&self.id]
		).map(|_|())
	}
}

