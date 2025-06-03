'use client';

import { useState, useEffect } from 'react';
import { getApiBaseUrl, getRaftState, setApiBaseUrl } from '../api/chatApi';

export default function Header() {
  const [serverUrl, setServerUrl] = useState('');
  const [nodeInfo, setNodeInfo] = useState<any>(null);
  const [showSettings, setShowSettings] = useState(false);

  useEffect(() => {
    setServerUrl(getApiBaseUrl());
    fetchRaftState();
  }, []);

  const fetchRaftState = async () => {
    try {
      const state = await getRaftState();
      setNodeInfo(state);
    } catch (error) {
      console.error('Failed to fetch Raft state:', error);
    }
  };

  const handleServerChange = () => {
    setApiBaseUrl(serverUrl);
    setShowSettings(false);
    fetchRaftState();
  };

  return (
    <header className="bg-gradient-to-r from-blue-700 to-purple-700 text-white p-4 shadow-md">
      <div className="container mx-auto flex justify-between items-center">
        <h1 className="text-xl font-bold">Distributed Chat Application</h1>
        
        <div className="flex items-center gap-3">
          {nodeInfo && (
            <div className="hidden md:flex text-xs bg-black/20 px-2 py-1 rounded-md">
              <span>Connected to Node: {nodeInfo.node_id}</span>
              <span className="mx-2">|</span>
              <span>State: {nodeInfo.current_state}</span>
              {nodeInfo.leader_id && (
                <>
                  <span className="mx-2">|</span>
                  <span>Leader: {nodeInfo.leader_id}</span>
                </>
              )}
            </div>
          )}
          
          <button 
            className="bg-white/20 hover:bg-white/30 text-white px-3 py-1 rounded-md text-sm transition-colors"
            onClick={() => setShowSettings(!showSettings)}
          >
            Settings
          </button>
        </div>
      </div>
      
      {showSettings && (
        <div className="absolute right-4 mt-2 bg-white text-gray-800 p-4 rounded-md shadow-lg z-10 w-80">
          <h3 className="font-semibold mb-2">Connection Settings</h3>
          <div className="flex flex-col gap-2">
            <label className="text-sm">
              Server URL:
              <input 
                type="text" 
                value={serverUrl} 
                onChange={(e) => setServerUrl(e.target.value)} 
                className="w-full p-2 border rounded mt-1 text-sm" 
                placeholder="http://localhost:5000"
              />
            </label>
            <div className="text-xs text-gray-500 mb-2">
              Enter the URL of any node in the cluster. The application will automatically connect to the leader.
            </div>
            <div className="flex justify-end gap-2">
              <button 
                className="bg-gray-200 hover:bg-gray-300 px-3 py-1 rounded text-sm"
                onClick={() => setShowSettings(false)}
              >
                Cancel
              </button>
              <button 
                className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm"
                onClick={handleServerChange}
              >
                Connect
              </button>
            </div>
          </div>
        </div>
      )}
    </header>
  );
}