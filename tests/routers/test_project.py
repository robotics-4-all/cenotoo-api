import uuid
from unittest.mock import AsyncMock, patch


class TestProjectEndpoints:
    """Tests for the project endpoints."""

    def test_create_project(self, client, sample_org_id):
        """Verify creating a project returns 200."""
        with (
            patch("routers.project.get_organization_id", return_value=sample_org_id),
            patch("routers.project.create_project_service", new_callable=AsyncMock) as mock_svc,
        ):
            mock_svc.return_value = {
                "message": "Project test_proj created succesfully",
                "id": str(uuid.uuid4()),
            }
            response = client.post(
                "/api/v1/projects",
                json={
                    "project_name": "test_proj",
                    "description": "A test project",
                    "tags": ["iot"],
                },
            )
        assert response.status_code == 200
        assert "message" in response.json()

    def test_get_all_projects(self, client, sample_org_id):
        """Verify getting all projects returns 200."""
        proj_id = uuid.uuid4()
        with (
            patch("routers.project.get_organization_id", return_value=sample_org_id),
            patch("routers.project.get_all_projects_service", new_callable=AsyncMock) as mock_svc,
        ):
            mock_svc.return_value = [
                {
                    "project_id": str(proj_id),
                    "organization_id": str(sample_org_id),
                    "organization_name": "test_org",
                    "project_name": "proj1",
                    "description": "First project",
                    "tags": [],
                    "creation_date": "2024-01-01",
                }
            ]
            response = client.get("/api/v1/projects")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert len(data["items"]) == 1
        assert data["total"] == 1

    def test_update_project(self, client, sample_org_id):
        """Verify updating a project returns 200."""
        del sample_org_id
        project_id = uuid.uuid4()
        with patch(
            "routers.project.update_project_service",
            new_callable=AsyncMock,
            return_value=None,
        ):
            response = client.put(
                f"/api/v1/projects/{project_id}",
                json={"description": "updated desc", "tags": ["new"]},
            )
        assert response.status_code == 200
        assert response.json()["message"] == "Project successfully updated"

    def test_get_project_by_id(self, client, sample_org_id):
        """Verify getting a project by ID returns 200."""
        project_id = uuid.uuid4()
        from models.project_models import ProjectResponse

        mock_response = ProjectResponse(
            project_id=project_id,
            organization_id=sample_org_id,
            organization_name="test_org",
            project_name="proj1",
            description="A project",
            tags=[],
            creation_date="2024-01-01",
        )
        with patch(
            "routers.project.get_project_by_id_service",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            response = client.get(f"/api/v1/projects/{project_id}")
        assert response.status_code == 200
        assert response.json()["project_name"] == "proj1"

    def test_delete_project(self, client, sample_org_id):
        """Verify deleting a project returns 200."""
        project_id = uuid.uuid4()
        with (
            patch("routers.project.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.project.delete_project_service",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            response = client.delete(f"/api/v1/projects/{project_id}")
        assert response.status_code == 200
        assert "message" in response.json()
