// API client for the distributed chat application

// Default to connecting to the first node's actual port
let API_BASE_URL = 'http://127.0.0.1:8080';

export interface Room {
  id: string;
  name: string;
  created_by: string;
  created_at: string;
}

export interface Message {
  id: string;
  room_id: string;
  user_id: string;
  content: string;
  message_type: string;
  timestamp: number;
}

export interface MessageCreate {
  id?: string; // Optional ID field, will be generated if not provided
  room_id: string;
  user_id: string;
  content: string;
}

export interface RoomCreate {
  name: string;
}

export const setApiBaseUrl = (url: string) => {
  API_BASE_URL = url;
};

export const getApiBaseUrl = () => {
  return API_BASE_URL;
};

// API functions
export const fetchRooms = async (): Promise<Room[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/rooms`);
    // Handle redirect to leader if needed (307 status)
    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        // Extract the new base URL from the redirect
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);
        // Retry with new base URL
        return fetchRooms();
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to fetch rooms: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error fetching rooms:', error);
    throw error;
  }
};

export const createRoom = async (room: RoomCreate): Promise<Room> => {
  try {
    // Add an ID field if not provided
    const roomData = {
      ...room,
      id: room.id || crypto.randomUUID()
    };

    const response = await fetch(`${API_BASE_URL}/rooms`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(roomData),
    });
    
    // Handle redirect to leader if needed
    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);
        // Retry with new base URL
        return createRoom(room);
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to create room: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error creating room:', error);
    throw error;
  }
};

export const fetchMessages = async (roomId: string): Promise<Message[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/rooms/${roomId}/messages`);
    
    // Handle redirect to leader if needed
    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);
        // Retry with new base URL
        return fetchMessages(roomId);
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to fetch messages: ${response.status}`);
    }
    
    // Process the response data
    const responseData = await response.json();
    let rawMessages: any[] = [];

    if (Array.isArray(responseData)) {
      rawMessages = responseData;
    } else if (responseData && Array.isArray(responseData.messages)) {
      rawMessages = responseData.messages;
    } else {
      console.warn('API response did not contain messages in expected format:', responseData);
      return [];
    }

    // Map raw messages to the Message interface
    return rawMessages.map((msg: any): Message => ({
      id: msg.message_id,
      room_id: msg.room_id,
      user_id: msg.user_id,
      content: msg.content,
      message_type: msg.message_type,
      timestamp: msg.created_at,
    }));
  } catch (error) {
    console.error(`Error fetching messages for room ${roomId}:`, error);
    throw error;
  }
};

export const sendMessage = async (message: MessageCreate): Promise<{ message_id: string }> => {
  try {
    // Add an ID field if not provided
    const messageData = {
      ...message,
      id: message.id || crypto.randomUUID()
    };

    const response = await fetch(`${API_BASE_URL}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(messageData),
    });
    
    // Handle redirect to leader if needed
    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);
        // Retry with new base URL
        return sendMessage(message);
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to send message: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error sending message:', error);
    throw error;
  }
};

export const getRaftState = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/raft_state`);
    
    if (!response.ok) {
      throw new Error(`Failed to get Raft state: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error getting Raft state:', error);
    throw error;
  }
};