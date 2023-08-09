def space_monitor(f):
    '''Decorator to calculate delta when a file is uploaded or removed from a bucket.
       Usage plagin uses it to check max storage space.
    '''
    def wrapper(*args, **kwargs):
        client = args[0]
        bucket = kwargs['bucket'] if kwargs.get('bucket') else args[1]
        filename = kwargs['filename'] if kwargs.get('filename') else args[-1]
        size_before = client.get_file_size(bucket, filename)
        result = f(*args, **kwargs)
        size_after = client.get_file_size(bucket, filename)
        payload = {
            'project_id': client.project.id if client.project else None,
            'current_delta': size_after - size_before, 
            'integration_id': client.integration_id,
            'is_local': client.is_local
        }
        client.event_manager.fire_event('usage_space_monitor', payload)
        return result
    return wrapper


def throughput_monitor(client, file_size: int, project_id: int = None):
    '''Function to calculate throughput when a file is read, uploaded or downloaded 
       from a bucket. Usage plagin uses it to check platform and project throughput.
    '''
    payload = {
        'project_id': client.project.id if client.project else project_id,
        'file_size': file_size, 
        'integration_id': client.integration_id,
        'is_local': client.is_local
    }
    client.event_manager.fire_event('usage_throughput_monitor', payload)
