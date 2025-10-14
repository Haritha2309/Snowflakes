use role accountadmin;

create notification integration MY_AZURE_NOTIFICATIONS
  enabled = true
  type = queue
  notification_provider = azure_storage_queue
  azure_storage_queue_primary_uri = 'https://hexsnowpipe.queue.core.windows.net/hexsnowpipequeue'
  azure_tenant_id = '7540734b-e567-46c3-9ad3-ec9fb9e50140';

  desc integration MY_AZURE_NOTIFICATIONS;

---- appname : txacrnsnowflakepacint