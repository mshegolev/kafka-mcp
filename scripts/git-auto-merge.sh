#!/bin/bash

# Удобная команда для запуска всего процесса автоматизации:
# коммит, пуш, проверка CI, мерж в мастер и бамп версии

set -e

echo "🚀 Git Auto Merge Process"
echo "========================"

# Если переданы аргументы, используем их как сообщение для коммита
if [ $# -gt 0 ]; then
    echo "📝 Committing changes with message: $*"
    git add .
    git commit -m "$*"
else
    echo "📝 Committing all changes"
    git add .
    git commit
fi

# Запускаем автоматизацию
./scripts/auto-merge-and-bump.sh

echo "🎉 Git auto merge process completed!"
