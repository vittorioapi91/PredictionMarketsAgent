import type { Metadata } from 'next';
import './styles/globals.css';
import './styles/layout.css';
import './styles/left-panel.css';
import './styles/search-bar.css';
import './styles/dashboard.css';

export const metadata: Metadata = {
  title: 'Polymarket Dashboard',
  description: 'Real-time market data and analytics',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
