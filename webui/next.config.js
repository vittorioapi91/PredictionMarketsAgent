/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Proxy API requests to FastAPI backend
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: 'http://localhost:7567/api/:path*',
      },
    ];
  },
};

module.exports = nextConfig;
