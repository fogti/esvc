use anyhow::{self as anyhow, anyhow as anyhow_, Context};
use esvc_traits::Engine;
use rayon::prelude::*;

#[derive(Clone)]
pub struct WasmEngine {
    wte: wasmtime::Engine,
    cmds: Vec<wasmtime::Module>,
}

impl Engine for WasmEngine {
    type Command = wasmtime::Module;
    type Error = anyhow::Error;
    type Arg = Vec<u8>;
    type Dat = Vec<u8>;

    fn run_event_bare(
        &self,
        cmd: &wasmtime::Module,
        arg: &Vec<u8>,
        dat: &Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        let datlen: i32 = dat
            .len()
            .try_into()
            .map_err(|_| anyhow_!("argument buffer overflow dat.len={}", dat.len()))?;
        let evarglen: i32 = arg
            .len()
            .try_into()
            .map_err(|_| anyhow_!("argument buffer overflow ev.arg.len={}", arg.len()))?;

        // WASM stuff

        let mut store = wasmtime::Store::new(&self.wte, ());
        let instance = wasmtime::Instance::new(&mut store, cmd, &[])?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow_!("unable to get export `memory`"))?;

        let retptr = instance
            .get_typed_func::<i32, i32, _>(&mut store, "__wbindgen_add_to_stack_pointer")?
            .call(&mut store, -16)?;
        let malloc = instance.get_typed_func::<i32, i32, _>(&mut store, "__wbindgen_malloc")?;
        //let free = instance.get_typed_func::<(i32, i32), (), _>(&mut store, "__wbindgen_free")?;

        // transform :: retptr:i32 -> evargptr:i32 -> evarglen:i32 -> datptr:i32 -> datlen:i32 -> ()
        let transform =
            instance.get_typed_func::<(i32, i32, i32, i32, i32), (), _>(&mut store, "transform")?;

        let evargptr = malloc.call(&mut store, evarglen)?;
        memory.write(&mut store, evargptr.try_into()?, arg)?;

        let datptr = malloc.call(&mut store, datlen)?;
        memory.write(&mut store, datptr.try_into()?, dat)?;

        // the main transform call
        let () = transform.call(&mut store, (retptr, evargptr, evarglen, datptr, datlen))?;

        // retrieve results
        let ret = {
            // *retptr :: (retptr2:i32, retlen2:i32)
            let mut retbuf = [0u8; 8];
            memory.read(&mut store, retptr.try_into()?, &mut retbuf)?;
            let (retp0, retp1) = retbuf.split_at(4);
            let retptr2: usize =
                i32::from_le_bytes(<[u8; 4]>::try_from(retp0).unwrap()).try_into()?;
            let retlen2: usize =
                i32::from_le_bytes(<[u8; 4]>::try_from(retp1).unwrap()).try_into()?;
            memory
                .data(&mut store)
                .get(retptr2..retptr2 + retlen2)
                .with_context(|| "return value length out of bounds".to_string())?
                .to_vec()
        };

        Ok(ret)
    }

    fn resolve_cmd(&self, cmd: u32) -> Option<&wasmtime::Module> {
        let cmd: usize = cmd.try_into().ok()?;
        self.cmds.get(cmd)
    }
}

impl WasmEngine {
    pub fn new() -> anyhow::Result<Self> {
        let wtc = wasmtime::Config::default();
        Ok(Self {
            wte: wasmtime::Engine::new(&wtc)?,
            cmds: Vec::new(),
        })
    }

    pub fn add_commands<II, Iter, Item>(&mut self, wasms: II) -> anyhow::Result<(u32, usize)>
    where
        II: IntoIterator<IntoIter = Iter>,
        Iter: Iterator<Item = Item> + Send,
        Item: AsRef<[u8]> + Send,
    {
        let orig_id = self.cmds.len();
        let id: u32 = orig_id.try_into()?;
        self.cmds.extend(
            wasms
                .into_iter()
                .par_bridge()
                .map(|cmd| wasmtime::Module::new(&self.wte, cmd))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok((id, self.cmds.len() - orig_id))
    }
}
