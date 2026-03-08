from collections import namedtuple
from datetime import datetime
from unittest.mock import AsyncMock, patch


class TestOrganizationEndpoints:
    """Tests for the organization endpoints."""

    def test_get_organization(self, client, sample_org_id):
        """Verify getting organization details returns 200."""
        OrgRow = namedtuple(
            "OrgRow",
            ["id", "organization_name", "description", "tags", "creation_date"],
        )
        mock_org = OrgRow(
            id=sample_org_id,
            organization_name="test_org",
            description="A test org",
            tags=["test"],
            creation_date=datetime.utcnow(),
        )
        with (
            patch("routers.organization.get_organization_id", return_value=sample_org_id),
            patch("routers.organization.check_organization_exists", return_value=mock_org),
            patch(
                "routers.organization.get_organization_info_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = {
                "organization_id": str(mock_org.id),
                "organization_name": "test_org",
                "description": "A test org",
                "tags": ["test"],
                "creation_date": str(mock_org.creation_date),
            }
            response = client.get("/api/v1/organization/nostradamus")
        assert response.status_code == 200

    def test_update_organization(self, client, sample_org_id):
        """Verify updating an organization returns 200."""
        with (
            patch("routers.organization.get_organization_id", return_value=sample_org_id),
            patch(
                "routers.organization.update_organization_service",
                new_callable=AsyncMock,
            ) as mock_svc,
        ):
            mock_svc.return_value = {"message": "Organization updated successfully."}
            response = client.put(
                "/api/v1/organization/nostradamus",
                json={"description": "Updated desc", "tags": ["updated"]},
            )
        assert response.status_code == 200
