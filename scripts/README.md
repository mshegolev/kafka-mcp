# Git Automation Scripts

This directory contains scripts to automate the git workflow for kafka-mcp development.

## Available Scripts

### 1. `auto-merge-and-bump.sh`
Main automation script that performs:
- Push changes to remote
- Wait for CI pipeline completion
- Merge branch to master
- Bump version number
- Commit new version

### 2. `git-auto-merge.sh`
Convenience script that:
- Commits all changes (with optional commit message)
- Runs the automation process

### 3. Git Hooks
- `post-commit` hook that automatically triggers the automation process after each commit

## Usage

### Manual Automation
```bash
# Commit changes and run full automation process
./scripts/git-auto-merge.sh "Your commit message"

# Or commit manually and then run automation
git add .
git commit -m "Your message"
./scripts/auto-merge-and-bump.sh
```

### Automatic Automation
After setting up the post-commit hook, the automation process will trigger automatically after each commit.

## Configuration

The scripts expect:
- A `VERSION` file in the project root containing the current version number (format: X.Y.Z)
- Git remote named `origin`
- Proper Git credentials configured for pushing

## Customization

You can customize the behavior by modifying:
- Version bumping logic in `auto-merge-and-bump.sh`
- CI pipeline checking logic in `auto-merge-and-bump.sh`
- Branch names and merge strategies
