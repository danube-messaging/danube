use crate::broker_server::DanubeServerImpl;

use danube_core::proto::{
    storage_service_server::StorageService, ListSegmentDescriptorsRequest,
    ListSegmentDescriptorsResponse, SegmentDescriptorProto,
};

use tonic::{Request, Response, Status};
use tracing::{debug, Level};

#[tonic::async_trait]
impl StorageService for DanubeServerImpl {
    /// Returns completed/sealed segment descriptors for a given topic.
    ///
    /// Used by `danube-iceberg` to discover new segments for Parquet conversion.
    /// Only segments with `completed = true` are returned. The `after_offset` field
    /// enables incremental polling (only segments with start_offset > after_offset).
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_segment_descriptors(
        &self,
        request: Request<ListSegmentDescriptorsRequest>,
    ) -> Result<Response<ListSegmentDescriptorsResponse>, Status> {
        let req = request.into_inner();

        debug!(
            topic = %req.topic,
            after_offset = req.after_offset,
            "list_segment_descriptors request"
        );

        let service = self.service.as_ref();

        let descriptors = service
            .topic_manager
            .storage_factory
            .list_segment_descriptors(&req.topic, req.after_offset)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "failed to list segment descriptors for topic {}: {}",
                    req.topic, e
                ))
            })?;

        let segments: Vec<SegmentDescriptorProto> = descriptors
            .into_iter()
            .map(|d| SegmentDescriptorProto {
                segment_id: d.segment_id,
                start_offset: d.start_offset,
                end_offset: d.end_offset,
                size: d.size,
                etag: d.etag.unwrap_or_default(),
                created_at: d.created_at,
            })
            .collect();

        debug!(
            topic = %req.topic,
            count = segments.len(),
            "list_segment_descriptors response"
        );

        Ok(Response::new(ListSegmentDescriptorsResponse { segments }))
    }
}
