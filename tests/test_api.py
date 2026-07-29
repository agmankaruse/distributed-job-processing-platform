def test_job_lifecycle(client, queue):
    response = client.post(
        "/v1/jobs",
        json={"task_type": "add_numbers", "payload": {"numbers": [2, 3, 5]}},
    )

    assert response.status_code == 201
    job = response.json()
    assert job["status"] == "queued"
    assert queue.claim() == job["id"]


def test_idempotency_key_returns_same_job(client):
    first = client.post(
        "/v1/jobs",
        headers={"Idempotency-Key": "invoice-42"},
        json={"task_type": "echo", "payload": {"value": "hello"}},
    )
    second = client.post(
        "/v1/jobs",
        headers={"Idempotency-Key": "invoice-42"},
        json={"task_type": "echo", "payload": {"value": "hello"}},
    )

    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]


def test_queued_job_can_be_cancelled(client):
    created = client.post(
        "/v1/jobs",
        json={"task_type": "sleep", "payload": {"seconds": 10}},
    ).json()

    response = client.delete(f"/v1/jobs/{created['id']}")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


def test_unknown_task_is_rejected(client):
    response = client.post(
        "/v1/jobs",
        json={"task_type": "send_money", "payload": {}},
    )

    assert response.status_code == 422
