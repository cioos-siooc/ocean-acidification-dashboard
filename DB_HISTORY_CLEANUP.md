DB history cleanup
===================

I removed the `DB/` directory from Git history to avoid committing large and sensitive database files. A backup bundle and branch were created before the rewrite:

- Backup branch: `backup/remove-db-before`
- Backup bundle: `../oa-backup.bundle`

If you are collaborating on this repository and the rewritten history is pushed to the remote, you will need to re-clone or reset your local clones. Recommended steps after the maintainer force-pushes the cleaned history:

Option A (recommended - re-clone):

    git clone <repo-url>

Option B (reset existing clone):

    git fetch origin --all
    git checkout main
    git reset --hard origin/main
    git clean -fdx

If you need to restore the old history locally, you can fetch the backup bundle:

    git clone ../oa-backup.bundle -b backup/remove-db-before

If you'd like me to perform the final force-push to your remote for you, provide the remote URL or run the push commands locally:

    git push --force --all
    git push --force --tags

Note: Force-pushing rewritten history will require all collaborators to re-clone or reset their local clones.
