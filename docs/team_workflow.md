# MeshWeaver - Team Git Workflow

## Branches

- main: Reviewed and integrated code
- feature/member-1-network: Network and P2P
- feature/member-2-broker: Broker and Scheduling
- feature/member-3-worker: Worker and Async Execution
- feature/member-4-integration: Integration, Storage and Quality

## Rules

1. Each member works on their own branch.
2. Do not commit directly to main.
3. Use meaningful commit messages.
4. Test changes before committing.
5. Push completed work to GitHub.
6. Open a pull request for integration.
7. Member 4 reviews changes before merging.
8. Keep README and documentation updated.

## Member 4 Review Checklist

- [ ] Check pull request changes.
- [ ] Check that tests pass.
- [ ] Check compatibility with shared models.
- [ ] Check for accidental dependency additions.
- [ ] Merge only after review.