'use client';

import { useState, useEffect } from 'react';
import Header from './components/Header';
import RoomList from './components/RoomList';
import ChatRoom from './components/ChatRoom';
import { Room } from './api/chatApi';

export default function Home() {
  const [selectedRoom, setSelectedRoom] = useState<Room | null>(null);
  const [userId, setUserId] = useState<string>('');
  
  useEffect(() => {
    // Generate a random user ID if not set
    if (!userId) {
      const storedUserId = localStorage.getItem('chatUserId');
      if (storedUserId) {
        setUserId(storedUserId);
      } else {
        const newUserId = `user_${Math.random().toString(36).substring(2, 8)}`;
        setUserId(newUserId);
        localStorage.setItem('chatUserId', newUserId);
      }
    }
  }, []);

  const handleSelectRoom = (room: Room) => {
    setSelectedRoom(room);
  };

  return (
    <div className="flex flex-col h-screen bg-white">
      <Header />
      
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar with room list */}
        <RoomList
          selectedRoom={selectedRoom}
          onSelectRoom={handleSelectRoom}
        />
        
        {/* Main chat area */}
        <div className="flex-1">
          {selectedRoom ? (
            <ChatRoom room={selectedRoom} userId={userId} />
          ) : (
            <div className="flex items-center justify-center h-full bg-gray-50">
              <div className="text-center p-8">
                <h2 className="text-2xl font-bold text-gray-700 mb-2">Welcome to Distributed Chat</h2>
                <p className="text-gray-500">Select a room to start chatting or create a new one.</p>
              </div>
            </div>
          )}
        </div>
      </div>
      
      {/* Connection status footer */}
      <footer className="bg-gray-100 text-xs text-gray-500 p-2 border-t border-gray-200">
        <div className="container mx-auto">
          <p>Connected as: {userId}</p>
          <p className="text-xs text-gray-400">
            This application connects to a distributed chat system running on Raft consensus.
          </p>
        </div>
      </footer>
    </div>
  );
}