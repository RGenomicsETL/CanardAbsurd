SELECT id FROM canard_absurd.tasks
WHERE id = ?id AND queue = ?queue AND name = ?name
    AND input::VARCHAR = ?input AND priority = ?priority AND max_failures = ?max_failures;
