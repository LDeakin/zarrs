pub struct TokioSpawner {
    handle: tokio::runtime::Handle,
}

impl TokioSpawner {
    #[must_use]
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }
}

impl futures::task::Spawn for TokioSpawner {
    fn spawn_obj(
        &self,
        future: futures::task::FutureObj<'static, ()>,
    ) -> Result<(), futures::task::SpawnError> {
        self.handle.spawn(future);
        Ok(())
    }
}
