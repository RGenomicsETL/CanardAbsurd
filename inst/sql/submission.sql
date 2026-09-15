SELECT CASE WHEN queue = ?queue AND name = ?name
    AND input IS NOT DISTINCT FROM ?input AND input_rtype = ?rtype
    AND priority = ?priority AND max_failures = ?max_failures
    THEN 1 ELSE 0 END AS changed, id
FROM canard_absurd.tasks WHERE id = ?id;
