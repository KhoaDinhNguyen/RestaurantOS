SELECT COALESCE(MAX(version), 0) + 1
FROM menu_versions;
