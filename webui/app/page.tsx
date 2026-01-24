'use client';

import { useState, useEffect, useRef } from 'react';

interface SearchResult {
  condition_id: string;
  question: string;
  description?: string;
  market_slug?: string;
  category?: string;
  [key: string]: any;
}

export default function Home() {
  const [searchVisible, setSearchVisible] = useState(false);
  const [searchHovered, setSearchHovered] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [showAllResults, setShowAllResults] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [activeNav, setActiveNav] = useState('portfolio');
  const [panelPinned, setPanelPinned] = useState(false);
  const searchTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const searchContainerRef = useRef<HTMLDivElement>(null);
  const searchResultsRef = useRef<HTMLDivElement>(null);

  const showSearch = () => {
    setSearchHovered(true);
  };

  const hideSearch = (e?: React.MouseEvent) => {
    // Only hide if search is not explicitly visible (clicked) and mouse is not over results
    if (!searchVisible) {
      const relatedTarget = e?.relatedTarget as HTMLElement;
      // If moving to search results, keep it visible
      if (relatedTarget && (
        searchContainerRef.current?.contains(relatedTarget) ||
        searchResultsRef.current?.contains(relatedTarget)
      )) {
        return;
      }
      setSearchHovered(false);
    }
  };

  const toggleSearch = () => {
    setSearchVisible(!searchVisible);
    if (!searchVisible) {
      setSearchHovered(true);
    } else {
      setSearchQuery('');
      setSearchResults([]);
    }
  };

  // Search function with debouncing
  useEffect(() => {
    if (searchQuery.length < 2) {
      setSearchResults([]);
      return;
    }

    // Clear previous timeout
    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current);
    }

    setIsSearching(true);

    // Debounce search by 300ms
    searchTimeoutRef.current = setTimeout(async () => {
      try {
        const response = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}&limit=10`);
        if (!response.ok) {
          throw new Error('Search failed');
        }
        const data = await response.json();
        setSearchResults(data.results || []);
      } catch (error) {
        console.error('Search error:', error);
        setSearchResults([]);
      } finally {
        setIsSearching(false);
      }
    }, 300);

    return () => {
      if (searchTimeoutRef.current) {
        clearTimeout(searchTimeoutRef.current);
      }
    };
  }, [searchQuery]);

  // Load more results
  const loadMoreResults = async () => {
    try {
      const response = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}&limit=100`);
      if (!response.ok) {
        throw new Error('Search failed');
      }
      const data = await response.json();
      setSearchResults(data.results || []);
      setShowAllResults(true);
    } catch (error) {
      console.error('Search error:', error);
    }
  };

  const navItems = [
    { id: 'portfolio', label: 'Portfolio', icon: '💼' },
    { id: 'watchlist', label: 'Watchlist', icon: '⭐' },
    { id: 'statistics', label: 'Statistics', icon: '📊' },
    { id: 'trade-history', label: 'Trade History', icon: '↩️' },
    { id: 'price-alerts', label: 'Price Alerts', icon: '🔔' },
  ];

  return (
    <div className="dashboard-layout">
      <div className="left-edge-hover-zone"></div>
      <aside className={`left-panel ${panelPinned ? 'pinned' : ''}`}>
        <div className="panel-header">
          <div className="panel-tile">
            <div className="tile-content">
              <span className="tile-title">Portfolio Builder</span>
              <span className="tile-subtitle">Market data and analytics</span>
            </div>
          </div>
          <button 
            className="pin-button"
            onClick={() => setPanelPinned(!panelPinned)}
            title={panelPinned ? "Unpin panel" : "Pin panel"}
          >
            {panelPinned ? '📌' : '📍'}
          </button>
        </div>
        <nav style={{ marginTop: '20px' }}>
          {navItems.map((item) => (
            <div
              key={item.id}
              className={`nav-item ${activeNav === item.id ? 'active' : ''}`}
              onClick={() => setActiveNav(item.id)}
            >
              <span className="nav-icon">{item.icon}</span>
              <span>{item.label}</span>
            </div>
          ))}
        </nav>
      </aside>
      <div className="container">
        <div className="top-bar">
          <button 
            className="search-icon-button"
            onClick={toggleSearch}
            onMouseEnter={showSearch}
            onMouseLeave={hideSearch}
          >
            🔍
          </button>
          {(searchVisible || searchHovered) && (
            <div 
              ref={searchContainerRef}
              className="search-container"
              onMouseEnter={showSearch}
              onMouseLeave={(e) => hideSearch(e)}
            >
              <input 
                type="text" 
                className="search-bar" 
                placeholder="Search markets..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                autoFocus={searchVisible || searchHovered}
              />
              {searchQuery.length >= 2 && (
                <div 
                  ref={searchResultsRef}
                  className="search-results"
                  onMouseEnter={showSearch}
                  onMouseLeave={hideSearch}
                >
                  {isSearching && (
                    <div className="search-loading">Searching...</div>
                  )}
                  {!isSearching && searchResults.length > 0 && (
                    <>
                      {searchResults.slice(0, showAllResults ? searchResults.length : 10).map((result, index) => (
                        <div 
                          key={result.condition_id || index} 
                          className="search-result-item"
                          onClick={() => {
                            console.log('Selected market:', result);
                          }}
                        >
                          <div className="result-question">{result.question || 'No title'}</div>
                          {result.description && (
                            <div className="result-description">
                              {result.description.length > 100 
                                ? result.description.substring(0, 100) + '...'
                                : result.description}
                            </div>
                          )}
                          {result.market_slug && (
                            <div className="result-slug">{result.market_slug}</div>
                          )}
                        </div>
                      ))}
                      {!showAllResults && searchResults.length === 10 && (
                        <div 
                          className="search-show-more"
                          onClick={loadMoreResults}
                        >
                          Show more results...
                        </div>
                      )}
                    </>
                  )}
                  {!isSearching && searchQuery.length >= 2 && searchResults.length === 0 && (
                    <div className="search-no-results">No results found</div>
                  )}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Dashboard Header with KPIs */}
        <div className="dashboard-header">
          <div className="kpi-container">
            <div className="kpi-item">
              <span className="kpi-label">Net Worth</span>
              <span className="kpi-value">$128,750</span>
              <span className="kpi-change positive">+5.2%</span>
            </div>
            <div className="kpi-item">
              <span className="kpi-label">Available Funds</span>
              <span className="kpi-value">$15,200</span>
            </div>
            <div className="kpi-item">
              <span className="kpi-label">Daily P&L</span>
              <span className="kpi-value">+$1,250</span>
              <span className="kpi-change positive">+2.1%</span>
            </div>
            <div className="kpi-item">
              <span className="kpi-label">Win Rate</span>
              <span className="kpi-value">78%</span>
              <span className="kpi-change positive">+3.2%</span>
              <div className="kpi-chart">
                {/* Placeholder for mini chart */}
              </div>
            </div>
          </div>
        </div>

        {/* Main Dashboard Grid */}
        <div className="dashboard-grid">
          {/* Order Book Panel */}
          <div className="dashboard-panel">
            <h2 className="panel-title">Order Book</h2>
            <div className="order-book-section">
              <div className="section-title buy">Buy Orders</div>
              <table className="order-book-table">
                <thead>
                  <tr>
                    <th>Price</th>
                    <th>Amount</th>
                    <th>Total</th>
                    <th>Sum</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="buy">
                    <td className="price">25.40</td>
                    <td>500</td>
                    <td>1,600</td>
                    <td>12,700</td>
                  </tr>
                  <tr className="buy">
                    <td className="price">25.38</td>
                    <td>300</td>
                    <td>300</td>
                    <td>7,614</td>
                  </tr>
                  <tr className="buy">
                    <td className="price">25.38</td>
                    <td>400</td>
                    <td>300</td>
                    <td>77,614</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div className="order-book-section">
              <div className="section-title sell">Sell Orders</div>
              <table className="order-book-table">
                <thead>
                  <tr>
                    <th>Price</th>
                    <th>Amount</th>
                    <th>Total</th>
                    <th>Sum</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="sell">
                    <td className="price">25.50</td>
                    <td>400</td>
                    <td>400</td>
                    <td>10,200</td>
                  </tr>
                  <tr className="sell">
                    <td className="price">25.52</td>
                    <td>600</td>
                    <td>600</td>
                    <td>15,312</td>
                  </tr>
                  <tr className="sell">
                    <td className="price">25.52</td>
                    <td>600</td>
                    <td>1,200</td>
                    <td>15,312</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* Market Chart Panel */}
          <div className="dashboard-panel">
            <h2 className="panel-title">Market Chart</h2>
            <div className="chart-container">
              <div className="chart-placeholder">
                Chart visualization will be implemented here
              </div>
            </div>
            <div className="volume-chart">
              {/* Volume chart placeholder */}
            </div>
          </div>
        </div>

        {/* Bottom Row */}
        <div className="dashboard-grid">
          {/* Recent Trades Panel */}
          <div className="dashboard-panel">
            <h2 className="panel-title">Recent Trades</h2>
            <table className="trades-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Price</th>
                  <th>Amount</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>14:25</td>
                  <td>25.48</td>
                  <td>250</td>
                </tr>
                <tr>
                  <td>14:22</td>
                  <td>25.46</td>
                  <td>500</td>
                </tr>
                <tr>
                  <td>14:23</td>
                  <td>25.46</td>
                  <td>500</td>
                </tr>
              </tbody>
            </table>
          </div>

          {/* Portfolio Stats Panel */}
          <div className="dashboard-panel">
            <h2 className="panel-title">Portfolio Stats</h2>
            <ul className="stats-list">
              <li className="stats-item">
                <span className="stats-label">Open Positions</span>
                <span className="stats-value">4</span>
              </li>
              <li className="stats-item">
                <span className="stats-label">Avg. Entry Price</span>
                <span className="stats-value">24.85</span>
              </li>
              <li className="stats-item">
                <span className="stats-label">Return</span>
                <span className="stats-value positive">+12.3%</span>
              </li>
            </ul>
            <div className="stats-chart">
              {/* Stats chart placeholder */}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
