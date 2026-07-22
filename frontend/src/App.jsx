import { useState, useEffect, useRef } from 'react';
import './index.css';

function App() {
    const [query, setQuery] = useState('');
    const [suggestions, setSuggestions] = useState([]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    
    // Search Results
    const [results, setResults] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [metrics, setMetrics] = useState({ latency: '--', shards: '--' });

    // Analytics
    const [popular, setPopular] = useState([]);
    const [recent, setRecent] = useState([]);
    const [health, setHealth] = useState({ online: false, text: 'Connecting...' });

    const debounceTimer = useRef(null);

    // Initial load & Polling
    useEffect(() => {
        const fetchDaemons = async () => {
            try {
                const aRes = await fetch('/analytics');
                const aData = await aRes.json();
                setPopular(aData.popular_queries || []);
                setRecent(aData.recent_queries || []);
            } catch (e) {
                console.warn("Analytics fetch failed");
            }

            try {
                const hRes = await fetch('/health');
                if (hRes.ok) {
                    setHealth({ online: true, text: 'Cluster Healthy' });
                } else {
                    setHealth({ online: false, text: 'Degraded' });
                }
            } catch (e) {
                setHealth({ online: false, text: 'Disconnected' });
            }
        };

        fetchDaemons();
        const t = setInterval(fetchDaemons, 5000);
        return () => clearInterval(t);
    }, []);

    // Autocomplete logic
    const handleQueryChange = (val) => {
        setQuery(val);
        if (val.trim().length === 0) {
            setShowSuggestions(false);
            return;
        }

        clearTimeout(debounceTimer.current);
        debounceTimer.current = setTimeout(async () => {
            try {
                const res = await fetch(`/suggest?q=${encodeURIComponent(val)}`);
                const data = await res.json();
                const items = data.suggestions || [];
                console.log(items);
                setSuggestions(items);
                setShowSuggestions(items.length > 0);
            } catch (e) {
                console.error(e);
            }
        }, 300);
    };

    // Perform Search
    const handleSearch = async (searchStr) => {
        if (!searchStr.trim()) return;
        setQuery(searchStr); // keep input in sync
        setShowSuggestions(false);
        setLoading(true);
        setError(null);

        let pathUrl = `/search?q=${encodeURIComponent(searchStr)}&highlight=true`;
        if (searchStr.includes('?')) {
            const parts = searchStr.split('?');
            pathUrl = `/search?q=${encodeURIComponent(parts[0].trim())}&highlight=true&${parts[1]}`;
        }

        try {
            const res = await fetch(pathUrl);
            const data = await res.json();
            console.log(data);
            setResults(data.results || []);
            setMetrics({
                latency: data.took_ms + (data.cached ? ' (Cached)' : ''),
                shards: data.shards_used ? data.shards_used.length : 0
            });
        } catch (e) {
            setError(e.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="app-container">
            {/* Sidebar Analytics */}
            <aside className="sidebar">
                <div className="logo">
                    <span className="logo-icon">🪐</span>
                    <h1>SearchSphere</h1>
                </div>

                <section className="cluster-health">
                    <h2>Cluster Status</h2>
                    <div className="status-indicator">
                        <span className={`dot ${health.online ? 'online' : 'offline'}`}></span>
                        <span>{health.text}</span>
                    </div>
                </section>

                <section className="analytics-card">
                    <h2>Popular Queries</h2>
                    <ul className="tag-list">
                        {popular.map((q, idx) => (
                            <li key={idx} onClick={() => handleSearch(q.query)}>{q.query}</li>
                        ))}
                    </ul>
                </section>

                <section className="analytics-card">
                    <h2>Recent Searches</h2>
                    <ul className="history-list">
                        {recent.map((r, idx) => (
                            <li key={idx}>{r.query} ({r.latency_ms}ms)</li>
                        ))}
                    </ul>
                </section>
            </aside>

            {/* Main Content */}
            <main className="main-content">
                <header className="search-header">
                    <div className="search-box">
                        <input
                            type="text"
                            value={query}
                            onChange={(e) => handleQueryChange(e.target.value)}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter') handleSearch(query);
                            }}
                            onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
                            onFocus={() => { if (suggestions.length > 0) setShowSuggestions(true); }}
                            placeholder="Search across shards (try metadata e.g., ?filter_category=...)"
                        />
                        <button onClick={() => handleSearch(query)}>Search</button>

                        {/* Autocomplete Dropdown */}
                        {showSuggestions && (
                            <div className="autocomplete-panel">
                                {suggestions.map((s, i) => (
                                    <div
                                        key={i}
                                        className="autocomplete-item"
                                        onClick={() => handleSearch(s.query)}
                                    >
                                        {s.query} <span style={{ opacity: 0.5, fontSize: '0.8em' }}>({s.score})</span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                    <div className="metrics-bar">
                        <span>Latency: {metrics.latency} ms</span>
                        <span>Shards: {metrics.shards}</span>
                    </div>
                </header>

                <div className="results-container">
                    {loading && <div className="empty-state">Loading...</div>}
                    {error && <div className="empty-state">Error: {error}</div>}
                    {!loading && !error && results === null && (
                        <div className="empty-state">
                            <span className="icon">🔍</span>
                            <p>Enter a query to search the cluster</p>
                        </div>
                    )}
                    {!loading && results && results.length === 0 && (
                        <div className="empty-state">
                            <span className="icon">🏜️</span>
                            <p>No results found.</p>
                        </div>
                    )}
                    {!loading && results && results.length > 0 && results.map((r) => {
                        const title = (r.highlights && r.highlights.title) ? r.highlights.title : r.title;
                        const content = (r.highlights && r.highlights.content) ? r.highlights.content : (r.content || '');
                        const metaTags = r.metadata 
                            ? Object.entries(r.metadata).map(([k, v]) => `${k}: ${v}`).join(' • ')
                            : '';

                        return (
                            <div key={r.id} className="result-card">
                                <h3 dangerouslySetInnerHTML={{ __html: title }}></h3>
                                <p dangerouslySetInnerHTML={{ __html: content }}></p>
                                <div className="result-meta">
                                    <span>ID: {r.id}</span>
                                    <span>Score: {r.score.toFixed(3)}</span>
                                    <span>Shard: {r.shard_id}</span>
                                    {metaTags && <span>| {metaTags}</span>}
                                </div>
                            </div>
                        );
                    })}
                </div>
            </main>
        </div>
    );
}

export default App;
