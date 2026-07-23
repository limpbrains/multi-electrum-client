import js from '@eslint/js';
import tseslint from '@typescript-eslint/eslint-plugin';
import tsparser from '@typescript-eslint/parser';
import prettier from 'eslint-config-prettier';

export default [
  js.configs.recommended,
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: tsparser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        project: './tsconfig.json',
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
    },
    rules: {
      ...tseslint.configs.recommended.rules,
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      '@typescript-eslint/consistent-type-imports': 'error',
      'no-console': ['warn', { allow: ['warn', 'error'] }],
      // TypeScript handles undefined identifiers; no-undef misclassifies platform globals
      // (setTimeout, MessageEvent, ...) that are typed via DOM / Node lib.
      'no-undef': 'off',
    },
  },
  {
    ignores: ['dist/**', 'coverage/**', 'node_modules/**', '*.config.js'],
  },
  {
    // Examples now participate in `pnpm typecheck` via tsconfig.examples.json,
    // but the main eslint flat config pins `parserOptions.project` to
    // ./tsconfig.json (which doesn't include examples/). Loosen parser
    // options for the examples folder so lint passes without the type-aware
    // rules that need a project. RN example depends on react / react-native
    // types we don't ship as devDeps — skip it in lint too.
    files: ['examples/**/*.ts', 'examples/**/*.tsx', 'docker/wsbridge/*.ts'],
    languageOptions: {
      parserOptions: {
        project: null,
      },
    },
    rules: {
      // Examples log to the console for clarity. That's the point.
      'no-console': 'off',
    },
  },
  {
    ignores: ['examples/rn-basic.tsx'],
  },
  prettier,
];
