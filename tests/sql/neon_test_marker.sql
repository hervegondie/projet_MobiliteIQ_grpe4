-- Uniquement sur ci-test-123, dans chacune des DEUX bases de test.
-- Ne jamais executer sur production.
SELECT current_database();
CREATE TABLE IF NOT EXISTS public.ci_test_environment (
    environment text PRIMARY KEY
);
INSERT INTO public.ci_test_environment(environment) VALUES ('ci-test-123')
ON CONFLICT DO NOTHING;
