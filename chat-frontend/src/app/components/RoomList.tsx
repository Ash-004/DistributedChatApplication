'use client';

import { useState, useEffect } from 'react';
import { Room, RoomCreate, createRoom, fetchRooms } from '../api/chatApi';

interface RoomListProps {
  selectedRoom: Room | null;
  onSelectRoom: (room: Room) => void;
}

export default function RoomList({ selectedRoom, onSelectRoom }: RoomListProps) {
  const [rooms, setRooms] = useState<Room[]>([]);
  const [newRoomName, setNewRoomName] = useState('');
  const [isCreatingRoom, setIsCreatingRoom] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadRooms();
  }, []);

  const loadRooms = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const roomList = await fetchRooms();
      setRooms(roomList);
      
      // Auto-select the first room if none is selected
      if (roomList.length > 0 && !selectedRoom) {
        onSelectRoom(roomList[0]);
      }
    } catch (error) {
      console.error('Failed to load rooms:', error);
      setError('Failed to load rooms. Please check your connection.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleCreateRoom = async () => {
    if (!newRoomName.trim()) return;
    
    try {
      setIsCreatingRoom(true);
      const roomData: RoomCreate = { name: newRoomName.trim() };
      const newRoom = await createRoom(roomData);
      setRooms([...rooms, newRoom]);
      setNewRoomName('');
      onSelectRoom(newRoom);
    } catch (error) {
      console.error('Failed to create room:', error);
      setError('Failed to create room. Please try again.');
    } finally {
      setIsCreatingRoom(false);
      setIsCreatingRoom(false);
    }
  };

  return (
    <div className="w-full md:w-64 bg-gray-100 border-r border-gray-200 flex flex-col h-full">
      <div className="p-4 border-b border-gray-200">
        <h2 className="font-semibold text-gray-800 mb-2">Chat Rooms</h2>
        
        {error && (
          <div className="bg-red-100 text-red-700 p-2 rounded mb-2 text-xs">
            {error}
            <button 
              className="ml-2 text-red-500 hover:text-red-700"
              onClick={loadRooms}
            >
              Retry
            </button>
          </div>
        )}
        
        <div className="flex gap-2">
          <input
            type="text"
            value={newRoomName}
            onChange={(e) => setNewRoomName(e.target.value)}
            placeholder="New room name"
            className="flex-1 p-2 border rounded text-sm"
            disabled={isCreatingRoom}
          />
          <button
            onClick={handleCreateRoom}
            disabled={!newRoomName.trim() || isCreatingRoom}
            className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm disabled:bg-blue-400"
          >
            Add
          </button>
        </div>
      </div>
      
      <div className="overflow-y-auto flex-1">
        {isLoading ? (
          <div className="flex justify-center items-center h-20">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
          </div>
        ) : rooms.length === 0 ? (
          <div className="p-4 text-center text-gray-500 text-sm">
            No rooms available. Create one to get started.
          </div>
        ) : (
          <ul>
            {rooms.map((room) => (
              <li key={room.id}>
                <button
                  onClick={() => onSelectRoom(room)}
                  className={`w-full text-left p-3 hover:bg-gray-200 transition-colors ${
                    selectedRoom?.id === room.id ? 'bg-blue-100 font-medium' : ''
                  }`}
                >
                  <div className="font-medium">{room.name}</div>
                  <div className="text-xs text-gray-500">
                    Created by: {room.created_by || 'Unknown'}
                  </div>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}