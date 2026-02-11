"""
Discovery Flask Routes - Add to your existing app.py

These routes provide the web interface and API for discovery.
"""

from flask import render_template, request, jsonify
import json
from datetime import datetime

# Import from your existing app
# from app import app, r

# Import the discovery task from tasks.py (NOT discovery_tasks)
from tasks import discover_instagram_profiles


@app.route('/discovery')
def discovery_page():
    """Discovery UI page"""
    return render_template('discovery.html')


@app.route('/api/discovery/instagram', methods=['POST'])
def start_instagram_discovery():
    """
    Start Instagram discovery job
    
    POST body (all optional except max_results has a default):
    {
        "max_results": 500,
        "follower_count": {"min": 20000, "max": 900000},
        "lookalike_type": "creator" | "audience",
        "lookalike_username": "@username",
        "creator_interests": ["Travel", "Fitness"],
        "hashtags": [{"name": "travel"}, {"name": "fitness"}]
    }
    
    Returns:
        202 Accepted with job_id
        400 Bad Request if validation fails
    """
    try:
        user_filters = request.json or {}
        
        # Validate max_results
        max_results = user_filters.get('max_results', 500)
        if not isinstance(max_results, int) or max_results < 1:
            return jsonify({'error': 'max_results must be a positive integer'}), 400
        if max_results > 4000:
            return jsonify({'error': 'max_results cannot exceed 4000'}), 400
        
        # Validate follower count
        follower_count = user_filters.get('follower_count', {})
        if follower_count:
            min_followers = follower_count.get('min')
            max_followers = follower_count.get('max')
            
            if min_followers and not isinstance(min_followers, int):
                return jsonify({'error': 'follower_count.min must be an integer'}), 400
            if max_followers and not isinstance(max_followers, int):
                return jsonify({'error': 'follower_count.max must be an integer'}), 400
            
            if min_followers and max_followers and min_followers >= max_followers:
                return jsonify({'error': 'follower_count.min must be less than max'}), 400
        
        # Validate lookalike (mutually exclusive)
        lookalike_type = user_filters.get('lookalike_type')
        lookalike_username = user_filters.get('lookalike_username', '').strip()
        
        if lookalike_type and lookalike_type not in ('creator', 'audience'):
            return jsonify({'error': 'lookalike_type must be "creator" or "audience"'}), 400
        
        if lookalike_type and not lookalike_username:
            return jsonify({'error': 'lookalike_username required when lookalike_type is set'}), 400
        
        # Queue discovery task
        task = discover_instagram_profiles.delay(user_filters=user_filters)
        job_id = str(task.id)
        
        # Initialize job tracking in Redis
        r.setex(
            f'discovery_job:{job_id}',
            86400,  # 24 hour TTL
            json.dumps({
                'job_id': job_id,
                'platform': 'instagram',
                'status': 'queued',
                'started_at': datetime.now().isoformat(),
                'filters': user_filters,
                'profiles_found': 0,
                'new_contacts_created': 0,
                'duplicates_skipped': 0
            })
        )
        
        return jsonify({
            'job_id': job_id,
            'status': 'queued'
        }), 202
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/discovery/jobs/<job_id>')
def get_discovery_job(job_id):
    """
    Get discovery job status and results
    
    Returns:
        200 OK with job data
        404 Not Found if job doesn't exist
    """
    job_data = r.get(f'discovery_job:{job_id}')
    
    if not job_data:
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify(json.loads(job_data))


@app.route('/api/discovery/jobs')
def list_discovery_jobs():
    """
    List recent discovery jobs
    
    Returns:
        200 OK with array of jobs (sorted by started_at desc)
    """
    job_keys = r.keys('discovery_job:*')
    jobs = []
    
    for key in job_keys:
        job_data = r.get(key)
        if job_data:
            try:
                jobs.append(json.loads(job_data))
            except json.JSONDecodeError:
                continue
    
    # Sort by started_at descending (most recent first)
    jobs.sort(key=lambda x: x.get('started_at', ''), reverse=True)
    
    return jsonify(jobs)


@app.route('/api/discovery/jobs/<job_id>', methods=['DELETE'])
def cancel_discovery_job(job_id):
    """
    Cancel a running discovery job (optional - for future enhancement)
    
    Currently just marks as cancelled in Redis, doesn't actually stop the task
    """
    job_data = r.get(f'discovery_job:{job_id}')
    
    if not job_data:
        return jsonify({'error': 'Job not found'}), 404
    
    job_data = json.loads(job_data)
    
    # Only allow cancellation of queued/discovering/importing jobs
    if job_data['status'] in ('completed', 'failed', 'cancelled'):
        return jsonify({'error': f'Cannot cancel job with status: {job_data["status"]}'}), 400
    
    # Update status to cancelled
    job_data['status'] = 'cancelled'
    job_data['updated_at'] = datetime.now().isoformat()
    
    r.setex(f'discovery_job:{job_id}', 86400, json.dumps(job_data))
    
    return jsonify({'status': 'cancelled'})


# Example integration with existing dashboard
@app.route('/api/stats/discovery')
def get_discovery_stats():
    """
    Get discovery statistics (optional - for dashboard integration)
    
    Returns aggregate stats across all discovery jobs
    """
    job_keys = r.keys('discovery_job:*')
    
    stats = {
        'total_jobs': 0,
        'completed_jobs': 0,
        'failed_jobs': 0,
        'running_jobs': 0,
        'total_profiles_found': 0,
        'total_contacts_created': 0,
        'total_duplicates_skipped': 0
    }
    
    for key in job_keys:
        job_data = r.get(key)
        if not job_data:
            continue
        
        try:
            job = json.loads(job_data)
            stats['total_jobs'] += 1
            
            if job.get('status') == 'completed':
                stats['completed_jobs'] += 1
                stats['total_profiles_found'] += job.get('profiles_found', 0)
                stats['total_contacts_created'] += job.get('new_contacts_created', 0)
                stats['total_duplicates_skipped'] += job.get('duplicates_skipped', 0)
            elif job.get('status') == 'failed':
                stats['failed_jobs'] += 1
            elif job.get('status') in ('queued', 'discovering', 'importing'):
                stats['running_jobs'] += 1
                
        except json.JSONDecodeError:
            continue
    
    return jsonify(stats)
