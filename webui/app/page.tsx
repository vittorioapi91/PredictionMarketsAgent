'use client';

import { useState, useEffect, useRef, useCallback } from 'react';

interface SearchResult {
  condition_id: string;
  question: string;
  description?: string;
  market_slug?: string;
  category?: string;
  token_0_id?: string;
  token_1_id?: string;
  [key: string]: any;
}

interface OrderBookLevel {
  price: string;
  size: string;
}

interface OrderBook {
  token_id: string;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
}

export default function Home() {
  const [searchVisible, setSearchVisible] = useState(false);
  const [searchHovered, setSearchHovered] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [showAllResults, setShowAllResults] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [includeInactive, setIncludeInactive] = useState(false);
  const [activeNav, setActiveNav] = useState('portfolio');
  const [panelPinned, setPanelPinned] = useState(false);
  const [selectedMarket, setSelectedMarket] = useState<SearchResult | null>(null);
  const [orderBooks, setOrderBooks] = useState<OrderBook[]>([]);
  const [isLoadingOrderBook, setIsLoadingOrderBook] = useState(false);
  const searchTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const searchContainerRef = useRef<HTMLDivElement>(null);
  const searchResultsRef = useRef<HTMLDivElement>(null);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);

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
        const response = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}&limit=10&include_inactive=${includeInactive}`);
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
  }, [searchQuery, includeInactive]);

  // Load more results
  const loadMoreResults = async () => {
    try {
      const response = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}&limit=100&include_inactive=${includeInactive}`);
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

  // Fetch order book data
  const fetchOrderBook = useCallback(async (conditionId: string) => {
    if (!conditionId) return;
    
    setIsLoadingOrderBook(true);
    try {
      const response = await fetch(`/api/markets/${conditionId}/orderbook`);
      if (!response.ok) {
        throw new Error('Failed to fetch order book');
      }
      const data = await response.json();
      
      // Transform order books data
      const transformedOrderBooks: OrderBook[] = (data.order_books || []).map((ob: any) => ({
        token_id: ob.token_id,
        bids: (ob.order_book?.bids || []).map((b: any) => ({
          price: typeof b.price === 'string' ? b.price : String(b.price),
          size: typeof b.size === 'string' ? b.size : String(b.size),
        })),
        asks: (ob.order_book?.asks || []).map((a: any) => ({
          price: typeof a.price === 'string' ? a.price : String(a.price),
          size: typeof a.size === 'string' ? a.size : String(a.size),
        })),
      }));
      
      setOrderBooks(transformedOrderBooks);
    } catch (error) {
      console.error('Error fetching order book:', error);
      setOrderBooks([]);
    } finally {
      setIsLoadingOrderBook(false);
    }
  }, []);

  // Poll order book when a market is selected
  useEffect(() => {
    if (selectedMarket?.condition_id) {
      // Fetch immediately
      fetchOrderBook(selectedMarket.condition_id);
      
      // Set up polling every 2 seconds
      pollingIntervalRef.current = setInterval(() => {
        fetchOrderBook(selectedMarket.condition_id);
      }, 2000);
      
      return () => {
        if (pollingIntervalRef.current) {
          clearInterval(pollingIntervalRef.current);
        }
      };
    } else {
      // Clear polling when no market is selected
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current);
        pollingIntervalRef.current = null;
      }
      setOrderBooks([]);
    }
  }, [selectedMarket, fetchOrderBook]);

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
                  <div style={{ 
                    padding: '8px 12px', 
                    borderBottom: '1px solid #333',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                    backgroundColor: '#2a2a2a'
                  }}>
                    <input
                      type="checkbox"
                      id="include-inactive"
                      checked={includeInactive}
                      onChange={(e) => setIncludeInactive(e.target.checked)}
                      style={{ cursor: 'pointer' }}
                    />
                    <label 
                      htmlFor="include-inactive"
                      style={{ 
                        color: '#999', 
                        fontSize: '12px',
                        cursor: 'pointer',
                        userSelect: 'none'
                      }}
                    >
                      Include inactive/closed/archived bets
                    </label>
                  </div>
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
                            setSelectedMarket(result);
                            setSearchVisible(false);
                            setSearchQuery('');
                            setSearchResults([]);
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

        {/* Order Book Panel - Only shown when a market is selected */}
        {selectedMarket && (
          <div className="dashboard-grid">
            <div className="dashboard-panel">
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
                <h2 className="panel-title">Order Book - {selectedMarket.question}</h2>
                <button 
                  onClick={() => {
                    setSelectedMarket(null);
                    setOrderBooks([]);
                  }}
                  style={{
                    background: 'transparent',
                    border: '1px solid #666',
                    color: '#fff',
                    padding: '8px 16px',
                    borderRadius: '4px',
                    cursor: 'pointer',
                  }}
                >
                  Close
                </button>
              </div>
              {isLoadingOrderBook && orderBooks.length === 0 && (
                <div style={{ textAlign: 'center', padding: '20px', color: '#999' }}>
                  Loading order book...
                </div>
              )}
              {!isLoadingOrderBook && orderBooks.length === 0 && (
                <div style={{ textAlign: 'center', padding: '20px', color: '#999' }}>
                  No order book data available
                </div>
              )}
              {orderBooks.map((orderBook, idx) => (
                <div key={orderBook.token_id || idx} style={{ marginBottom: '30px' }}>
                  {orderBooks.length > 1 && (
                    <h3 style={{ color: '#999', fontSize: '14px', marginBottom: '10px' }}>
                      Token: {orderBook.token_id}
                    </h3>
                  )}
                  <div className="order-book-section">
                    <div className="section-title buy">Buy Orders (Bids)</div>
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
                        {orderBook.bids.length === 0 ? (
                          <tr>
                            <td colSpan={4} style={{ textAlign: 'center', color: '#999', padding: '20px' }}>
                              No bids available
                            </td>
                          </tr>
                        ) : (
                          orderBook.bids.map((bid, bidIdx) => {
                            const price = parseFloat(bid.price);
                            const size = parseFloat(bid.size);
                            const total = price * size;
                            const cumulativeSum = orderBook.bids
                              .slice(0, bidIdx + 1)
                              .reduce((sum, b) => sum + parseFloat(b.price) * parseFloat(b.size), 0);
                            
                            return (
                              <tr key={bidIdx} className="buy">
                                <td className="price">{price.toFixed(2)}</td>
                                <td>{size.toLocaleString()}</td>
                                <td>{total.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
                                <td>{cumulativeSum.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
                              </tr>
                            );
                          })
                        )}
                      </tbody>
                    </table>
                  </div>
                  <div className="order-book-section">
                    <div className="section-title sell">Sell Orders (Asks)</div>
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
                        {orderBook.asks.length === 0 ? (
                          <tr>
                            <td colSpan={4} style={{ textAlign: 'center', color: '#999', padding: '20px' }}>
                              No asks available
                            </td>
                          </tr>
                        ) : (
                          orderBook.asks.map((ask, askIdx) => {
                            const price = parseFloat(ask.price);
                            const size = parseFloat(ask.size);
                            const total = price * size;
                            const cumulativeSum = orderBook.asks
                              .slice(0, askIdx + 1)
                              .reduce((sum, a) => sum + parseFloat(a.price) * parseFloat(a.size), 0);
                            
                            return (
                              <tr key={askIdx} className="sell">
                                <td className="price">{price.toFixed(2)}</td>
                                <td>{size.toLocaleString()}</td>
                                <td>{total.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
                                <td>{cumulativeSum.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
                              </tr>
                            );
                          })
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Main Dashboard Grid - Only show when no market is selected */}
        {!selectedMarket && (
          <>
            <div className="dashboard-grid">
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
          </>
        )}

        {/* Bottom Row - Only show when no market is selected */}
        {!selectedMarket && (
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
        )}
      </div>
    </div>
  );
}
