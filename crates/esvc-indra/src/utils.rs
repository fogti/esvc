use esvc_core::{to_bytes, uuid_from_hash, EventWithDeps};
use indradb::{Identifier, VertexQueryExt};

#[pyo3::pyfunction]
pub fn id_to_base32(id: u128) -> String {
    base32::encode(
        base32::Alphabet::RFC4648 { padding: false },
        &id.to_be_bytes(),
    )
}

pub fn base32_to_id(b32: &str) -> Option<u128> {
    let v = base32::decode(base32::Alphabet::RFC4648 { padding: false }, b32)?;
    const BLKLEN: usize = 16;
    if v.len() > BLKLEN {
        return None;
    }
    let mut d16 = [0u8; BLKLEN];
    if !v.is_empty() {
        d16[BLKLEN - v.len()..].copy_from_slice(&v[..]);
    }
    Some(u128::from_be_bytes(d16))
}

pub fn ensure_node(
    ds: &dyn indradb::Datastore,
    evwd: &EventWithDeps,
) -> Result<u128, indradb::Error> {
    // calculate hash and use it as id
    let id =
        uuid_from_hash(&to_bytes::<_, 256>(evwd).map_err(|e| indradb::Error::Datastore(e.into()))?);
    let idqry: indradb::VertexQuery = indradb::SpecificVertexQuery::single(id).into();
    let t = Identifier::new(id_to_base32(evwd.ev.name)).unwrap();

    let done_key = Identifier::new("__ready__").unwrap();
    let arg_key = Identifier::new("__arg__").unwrap();
    let empty_key = Identifier::new("").unwrap();

    ds.index_property(done_key.clone())?;

    if ds.create_vertex(&indradb::Vertex { t: t.clone(), id })?
        || ds.get_vertex_properties(indradb::VertexPropertyQuery {
            name: done_key.clone(),
            inner: idqry.clone(),
        })? != vec![indradb::VertexProperty {
            id,
            value: serde_json::json!(true),
        }]
    {
        // vertex newly created
        use core::iter::once;
        use indradb::BulkInsertItem as Item;
        ds.delete_edges(indradb::SpecificVertexQuery::single(id).outbound().into())?;
        ds.bulk_insert(
            evwd.deps
                .iter()
                .map(|&v| {
                    Item::Edge(indradb::EdgeKey {
                        outbound_id: id,
                        inbound_id: uuid::Uuid::from_u128(v),
                        t: empty_key.clone(),
                    })
                })
                .chain(once(Item::VertexProperty(
                    id,
                    arg_key,
                    serde_json::json!(&evwd.ev.arg),
                )))
                .chain(once(Item::VertexProperty(
                    id,
                    done_key,
                    serde_json::json!(true),
                )))
                .collect(),
        )?;
    } else {
        // TODO: warn about all hash conflicts
        let err = Err(indradb::Error::UuidTaken);
        if let [vtx] = &ds.get_vertices(idqry.clone())?[..] {
            if vtx.t != t {
                return err;
            }
        } else {
            unreachable!();
        }
        if let [vtp] = &ds.get_vertex_properties(indradb::VertexPropertyQuery {
            name: arg_key,
            inner: idqry,
        })?[..]
        {
            if vtp.value != serde_json::json!(&evwd.ev.arg) {
                return err;
            }
        } else {
            unreachable!();
        }
    }
    Ok(id.as_u128())
}

#[allow(unused)]
pub fn replace_node(
    ds: &dyn indradb::Datastore,
    old: u128,
    new: u128,
) -> Result<(), indradb::Error> {
    if old == new {
        return Ok(());
    }
    // this changes all edges [pointing at old] to [pointing at new]
    // and discards the outgoing edges (dependency relations) of [old]

    let old_inbound_id = uuid::Uuid::from_u128(old);
    let new_inbound_id = uuid::Uuid::from_u128(new);
    let new_inbound_edges = ds
        .get_edges(
            indradb::SpecificVertexQuery::single(old_inbound_id)
                .inbound()
                .into(),
        )?
        .into_iter()
        .map(|e| {
            let mut ret = e.key;
            ret.inbound_id = new_inbound_id;
            ret
        })
        .map(indradb::BulkInsertItem::Edge)
        .collect::<Vec<_>>();
    ds.bulk_insert(new_inbound_edges)?;
    ds.delete_vertices(indradb::SpecificVertexQuery::single(old_inbound_id).into())?;
    Ok(())
}

pub fn get_event(ds: &dyn indradb::Datastore, eid: u128) -> Result<EventWithDeps, indradb::Error> {
    let eidqry = indradb::SpecificVertexQuery::single(uuid::Uuid::from_u128(eid));
    if let [pl1] = &ds.get_all_vertex_properties(eidqry.clone().into())?[..] {
        let props: std::collections::HashMap<_, _> = pl1
            .props
            .iter()
            .map(|indradb::NamedProperty { name, value }| (name.to_string(), value))
            .collect();
        if !props.contains_key("__ready__") {
            return Err(indradb::Error::Datastore(
                "event not ready".to_string().into(),
            ));
        }
        let arg: Vec<u8> =
            serde_json::from_value((*props.get("__arg__").expect("event argument")).clone())
                .expect("event argument deserialization");
        let deps: std::collections::BTreeSet<u128> = ds
            .get_edges(eidqry.outbound().into())?
            .into_iter()
            .map(|i| i.key.inbound_id.as_u128())
            .collect();

        Ok(EventWithDeps {
            ev: esvc_core::Event {
                name: base32_to_id(pl1.vertex.t.as_str()).expect("invalid event name"),
                arg,
            },
            deps,
        })
    } else {
        Err(indradb::Error::Datastore(
            "event not found".to_string().into(),
        ))
    }
}
