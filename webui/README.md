# Polymarket Dashboard - Next.js

This is the Next.js + TypeScript frontend for the Polymarket Dashboard.

## Setup

1. Install dependencies:
```bash
npm install
# or
yarn install
# or
pnpm install
```

2. Make sure the FastAPI backend is running on `http://localhost:7567`

3. Start the development server:
```bash
npm run dev
# or
yarn dev
# or
pnpm dev
```

The dashboard will be available at `http://localhost:3782`

## Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run start` - Start production server
- `npm run lint` - Run ESLint
- `npm run type-check` - Run TypeScript type checking

## Architecture

- **Next.js 14** with App Router
- **TypeScript** for type safety
- **React Server Components** and Client Components
- API calls are proxied to the FastAPI backend via `next.config.js` rewrites

## Project Structure

```
webui/
├── app/
│   ├── layout.tsx      # Root layout
│   ├── page.tsx        # Main dashboard page
│   └── globals.css     # Global styles
├── package.json        # Dependencies
├── tsconfig.json       # TypeScript config
├── next.config.js      # Next.js config
└── .eslintrc.json      # ESLint config
```

## API Integration

The dashboard communicates with the FastAPI backend running on port 7567. API requests are automatically proxied through Next.js rewrites configured in `next.config.js`.

## Production Build

To build for production:

```bash
npm run build
npm run start
```

For production deployment, you may want to:
1. Configure environment variables
2. Set up proper API URL (not localhost)
3. Configure CORS on the FastAPI backend if needed
