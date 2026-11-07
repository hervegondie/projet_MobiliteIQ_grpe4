"""Ces protections sont testables sans réseau ni connexion à Neon."""
import unittest
from unittest.mock import patch


class NeonGuardTests(unittest.TestCase):
    def test_no_test_host_is_refused(self):
        from neon_support import validate_settings
        with self.assertRaisesRegex(ValueError,'TEST_NEON_HOST'):
            validate_settings({})

    def test_production_hostname_is_refused(self):
        from neon_support import validate_settings
        env={'TEST_NEON_HOST':'test.invalid',
             'TEST_BRONZE_DATABASE_URL':'postgresql://example:demo@production.invalid/mobiliteIQIngestion?sslmode=require',
             'TEST_SILVER_DATABASE_URL':'postgresql://example:demo@test.invalid/Silver_test?sslmode=require'}
        with self.assertRaisesRegex(ValueError,'hôte de test'):
            validate_settings(env)

    def test_wrong_database_is_refused(self):
        from neon_support import validate_settings
        env={'TEST_NEON_HOST':'test.invalid',
             'TEST_BRONZE_DATABASE_URL':'postgresql://example:demo@test.invalid/neondb?sslmode=require',
             'TEST_SILVER_DATABASE_URL':'postgresql://example:demo@test.invalid/Silver_test?sslmode=require'}
        with self.assertRaisesRegex(ValueError,'base attendus'):
            validate_settings(env)

    def test_tls_is_required(self):
        from neon_support import validate_settings
        env={'TEST_NEON_HOST':'test.invalid',
             'TEST_BRONZE_DATABASE_URL':'postgresql://example:demo@test.invalid/mobiliteIQIngestion?sslmode=disable',
             'TEST_SILVER_DATABASE_URL':'postgresql://example:demo@test.invalid/Silver_test?sslmode=require'}
        with self.assertRaisesRegex(ValueError,'TLS'):
            validate_settings(env)
